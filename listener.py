import sys
import os
import time
import json
import queue
import threading
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler
from socketserver import ThreadingTCPServer
from pynput import keyboard
from pymongo import MongoClient

# Configuration
PORT = 5001
ACTIVITY_FILE = 'activity.json'
IDLE_TIMEOUT = 2.0 # Wait 2 seconds before committing a sentence
PASSCODE = "9505"

# MongoDB Configuration - Read from environment variable for security
if os.path.exists('.env'):
    try:
        with open('.env', 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip().strip('"').strip("'")
    except Exception as e:
        print(f"Error reading local .env file: {e}")

MONGO_URI = os.environ.get("MONGO_URI")
mongo_connected = False
db_history_col = None


if MONGO_URI:
    try:
        print("Connecting to MongoDB Atlas...")
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo_client.admin.command('ping')
        db = mongo_client['TypeCraft']
        db_history_col = db['stream_history']
        mongo_connected = True
        print("Successfully connected to MongoDB Atlas!")
    except Exception as e:
        print(f"MongoDB connection failed: {e}. Falling back to local storage.")
        mongo_connected = False
else:
    print("MONGO_URI environment variable not found. Running in local-only mode.")
    mongo_connected = False


# Background DB Queue Worker
db_queue = queue.Queue()

def db_worker():
    global mongo_connected
    while True:
        task = db_queue.get()
        if task is None:
            break
            
        action, data = task
        
        if action == "update_active":
            text = data["text"]
            timestamp = data["timestamp"]
            
            # Save local active cache if needed (omitted here to keep local filesystem overhead minimal)
            if mongo_connected:
                try:
                    db_history_col.update_one(
                        {"active": True},
                        {"$set": {"text": text, "timestamp": timestamp}},
                        upsert=True
                    )
                except Exception as e:
                    print(f"MongoDB real-time save failed: {e}")
                    
        elif action == "commit_active":
            text = data["text"]
            timestamp = data["timestamp"]
            
            # Format: Strip trailing period if present
            text = text.strip()
            if text.endswith('.'):
                text = text[:-1].strip()
                
            if text:
                record = {
                    "timestamp": timestamp,
                    "text": text
                }
                
                # Update/Upsert the active document and mark it as completed (active: False)
                if mongo_connected:
                    try:
                        db_history_col.update_one(
                            {"active": True},
                            {"$set": {"text": text, "timestamp": timestamp, "active": False}}
                        )
                        print(f"Committed to MongoDB (Period Stripped): {text}")
                    except Exception as e:
                        print(f"MongoDB commit failed: {e}. Falling back to local append.")
                        
                # Save to local file backup
                save_local_record(record)
                
                # Delete _id object from MongoDB record for JSON serialization
                if '_id' in record:
                    del record['_id']
                # Broadcast commit event to web app
                broadcast('commit', record)
                
        elif action == "clear_db":
            if mongo_connected:
                try:
                    db_history_col.delete_many({})
                    print("Wiped remote MongoDB collection")
                except Exception as e:
                    print(f"MongoDB clear failed: {e}")
            broadcast('clear', {})
            
        db_queue.task_done()

def save_local_record(record):
    try:
        with open(ACTIVITY_FILE, 'r', encoding='utf-8') as f:
            local_history = json.load(f)
    except Exception:
        local_history = []
        
    local_history.append(record)
    try:
        with open(ACTIVITY_FILE, 'w', encoding='utf-8') as f:
            json.dump(local_history, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error saving local backup: {e}")

# Global State
clients = []
clients_lock = threading.Lock()

active_buffer = ""
buffer_lock = threading.Lock()
last_typed_time = time.time()

def broadcast(event_type, data):
    payload = f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
    encoded_payload = payload.encode('utf-8')
    
    with clients_lock:
        disconnected = []
        for client in clients:
            try:
                client.wfile.write(encoded_payload)
                client.wfile.flush()
            except Exception:
                disconnected.append(client)
                
        for client in disconnected:
            if client in clients:
                clients.remove(client)

def check_idle_loop():
    global active_buffer, last_typed_time
    while True:
        time.sleep(0.5)
        with buffer_lock:
            if active_buffer and (time.time() - last_typed_time > IDLE_TIMEOUT):
                db_queue.put(("commit_active", {"text": active_buffer, "timestamp": int(time.time() * 1000)}))
                active_buffer = ""
                broadcast('live', {"text": ""})

# SSE HTTP Handler
class SSEHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'X-Requested-With, Content-Type')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        global clients
        
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)
        passcode_input = query_params.get('passcode', [None])[0]
        
        # Security passcode gate
        if passcode_input != PASSCODE:
            self.send_response(401)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Unauthorized. Invalid passcode."}).encode('utf-8'))
            return

        # Handle SSE Event Source Stream
        if parsed_url.path == '/stream':
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Connection', 'keep-alive')
            self.end_headers()
            
            with clients_lock:
                clients.append(self)
                
            # Send initial backlog from MongoDB or Local File
            backlog = []
            if mongo_connected:
                try:
                    # Fetch only completed records (active: False or not exist)
                    cursor = db_history_col.find({"active": {"$ne": True}}, {'_id': False}).sort('timestamp', -1).limit(50)
                    backlog = list(cursor)
                    backlog.reverse()
                except Exception as e:
                    print(f"Error reading from MongoDB backlog: {e}")
            
            # Local fallback if MongoDB failed or offline
            if not backlog:
                try:
                    with open(ACTIVITY_FILE, 'r', encoding='utf-8') as f:
                        lh = json.load(f)
                    backlog = lh[-50:]
                except Exception:
                    backlog = []
            
            try:
                self.wfile.write(f"event: init\ndata: {json.dumps(backlog, ensure_ascii=False)}\n\n".encode('utf-8'))
                self.wfile.flush()
            except Exception:
                with clients_lock:
                    if self in clients:
                        clients.remove(self)
                return

            while True:
                try:
                    time.sleep(5)
                    self.wfile.write(": ping\n\n".encode('utf-8'))
                    self.wfile.flush()
                except Exception:
                    break
            
            with clients_lock:
                if self in clients:
                    clients.remove(self)
                    
        # Serve database activity log
        elif parsed_url.path == '/activity':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            
            history_data = []
            if mongo_connected:
                try:
                    cursor = db_history_col.find({"active": {"$ne": True}}, {'_id': False}).sort('timestamp', -1)
                    history_data = list(cursor)
                except Exception as e:
                    print(f"Failed to query MongoDB history: {e}")
                    
            if not history_data:
                try:
                    with open(ACTIVITY_FILE, 'r', encoding='utf-8') as f:
                        lh = json.load(f)
                    history_data = sorted(lh, key=lambda x: x['timestamp'], reverse=True)
                except Exception:
                    history_data = []
                    
            self.wfile.write(json.dumps(history_data, ensure_ascii=False).encode('utf-8'))
            
        elif parsed_url.path == '/clear':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            
            # Push clear action to queue
            db_queue.put(("clear_db", None))
            
            # Clear Local History Backup
            try:
                with open(ACTIVITY_FILE, 'w', encoding='utf-8') as f:
                    json.dump([], f)
            except Exception:
                pass
                    
            self.wfile.write(json.dumps({"status": "cleared"}).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

# Keyboard Input Processing
def on_press(key):
    global active_buffer, last_typed_time
    
    char = None
    try:
        # Intercept standard character keys
        if hasattr(key, 'char') and key.char is not None:
            char = key.char
        # Intercept virtual codes (numpad 0-9 keys are vk codes 96-105)
        elif hasattr(key, 'vk') and 96 <= key.vk <= 105:
            char = str(key.vk - 96)
        # Intercept numpad decimal dot key (vk 110)
        elif hasattr(key, 'vk') and key.vk == 110:
            char = "."
        # Intercept space and enters
        elif key == keyboard.Key.space:
            char = " "
        elif key == keyboard.Key.enter:
            char = "\n"
    except Exception:
        pass

    with buffer_lock:
        last_typed_time = time.time()
        
        if key == keyboard.Key.backspace:
            if active_buffer:
                active_buffer = active_buffer[:-1]
                # Broadcast live updates instantly
                broadcast('live', {"text": active_buffer})
                # Push real-time save update to non-blocking queue
                db_queue.put(("update_active", {"text": active_buffer, "timestamp": int(time.time() * 1000)}))
        elif char is not None:
            if char == "\n":
                if active_buffer.strip():
                    db_queue.put(("commit_active", {"text": active_buffer, "timestamp": int(time.time() * 1000)}))
                    active_buffer = ""
                    broadcast('live', {"text": ""})
            else:
                active_buffer += char
                broadcast('live', {"text": active_buffer})
                
                # Push active save to non-blocking queue
                db_queue.put(("update_active", {"text": active_buffer, "timestamp": int(time.time() * 1000)}))
                
                # Sentence completed instantly on punctuation marks
                if char in ['.', '?', '!']:
                    # Trigger delay to complete the sentence commit
                    threading.Thread(target=commit_after_short_delay, args=(active_buffer,)).start()
                    active_buffer = ""

def commit_after_short_delay(text_to_commit):
    time.sleep(0.3)
    db_queue.put(("commit_active", {"text": text_to_commit, "timestamp": int(time.time() * 1000)}))

class ThreadedHTTPServer(ThreadingTCPServer):
    allow_reuse_address = True

def main():
    # Start background database worker thread
    threading.Thread(target=db_worker, daemon=True).start()
    
    # Start background idle monitor timer thread
    threading.Thread(target=check_idle_loop, daemon=True).start()
    
    # Check if running locally on Windows with keyboard capabilities
    # Skip hook if '--server-only' is passed (used for hosting API server in the cloud on Render)
    if sys.platform == 'win32' and '--server-only' not in sys.argv:
        print("Starting global keyboard background listener...")
        listener = keyboard.Listener(on_press=on_press)
        listener.start()
    else:
        print("Running in server-only mode (API Server active, no keyboard hook)...")
        listener = None
        
    server = ThreadedHTTPServer(('0.0.0.0', PORT), SSEHandler)
    print(f"Real-Time Stream Server listening on port {PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        if listener:
            listener.stop()
        server.shutdown()
        db_queue.put(None) # stop queue
        sys.exit(0)

if __name__ == '__main__':
    main()
