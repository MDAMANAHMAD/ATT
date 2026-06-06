import sys
import time
import json
import threading
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler
from socketserver import ThreadingTCPServer
from pynput import keyboard
from pymongo import MongoClient

# Configuration
PORT = 5001
ACTIVITY_FILE = 'activity.json'
IDLE_TIMEOUT = 4.0 # Seconds before committing sentence automatically
PASSCODE = "9505"

# MongoDB Configuration
MONGO_URI = "mongodb+srv://amanahmad0406_db_user:i7w66LVt7JXGP86x@cluster0.z2bsgc5.mongodb.net/?appName=Cluster0"
mongo_connected = False
db_history_col = None

try:
    print("Connecting to MongoDB Atlas...")
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Test connection
    mongo_client.admin.command('ping')
    db = mongo_client['TypeCraft']
    db_history_col = db['stream_history']
    mongo_connected = True
    print("Successfully connected to MongoDB Atlas!")
except Exception as e:
    print(f"MongoDB connection failed: {e}. Falling back to local storage.")
    mongo_connected = False

# Global State
clients = []
clients_lock = threading.Lock()

active_buffer = ""
buffer_lock = threading.Lock()
last_typed_time = time.time()

# Load existing local history or create empty list
try:
    with open(ACTIVITY_FILE, 'r', encoding='utf-8') as f:
        local_history = json.load(f)
except Exception:
    local_history = []

local_history_lock = threading.Lock()

def save_sentence(text):
    global active_buffer
    text = text.strip()
    if not text:
        return
        
    record = {
        "timestamp": int(time.time() * 1000),
        "text": text
    }
    
    # Save to MongoDB
    if mongo_connected:
        try:
            db_history_col.insert_one(record.copy())
            print(f"Saved to MongoDB: {text}")
        except Exception as e:
            print(f"Failed to save to MongoDB: {e}. Saving locally.")
            
    # Always save to local backup
    with local_history_lock:
        local_history.append(record)
        try:
            with open(ACTIVITY_FILE, 'w', encoding='utf-8') as f:
                json.dump(local_history, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving local backup: {e}")
            
    # Remove _id object from MongoDB record for JSON serialization
    if '_id' in record:
        del record['_id']
        
    broadcast('commit', record)

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
        time.sleep(1.0)
        with buffer_lock:
            if active_buffer and (time.time() - last_typed_time > IDLE_TIMEOUT):
                save_sentence(active_buffer)
                active_buffer = ""
                broadcast('live', {"text": ""})

# SSE HTTP Handler
class SSEHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Mute logging in console
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
        
        # Security Passcode Gate
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
                    cursor = db_history_col.find({}, {'_id': False}).sort('timestamp', -1).limit(50)
                    backlog = list(cursor)
                    backlog.reverse() # send chronological
                except Exception as e:
                    print(f"Error reading from MongoDB backlog: {e}")
            
            # Local fallback if MongoDB failed or offline
            if not backlog:
                with local_history_lock:
                    backlog = local_history[-50:]
            
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
                    cursor = db_history_col.find({}, {'_id': False}).sort('timestamp', -1)
                    history_data = list(cursor)
                except Exception as e:
                    print(f"Failed to query MongoDB history: {e}")
                    
            if not history_data:
                with local_history_lock:
                    history_data = sorted(local_history, key=lambda x: x['timestamp'], reverse=True)
                    
            self.wfile.write(json.dumps(history_data, ensure_ascii=False).encode('utf-8'))
            
        elif parsed_url.path == '/clear':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            
            # Clear MongoDB
            if mongo_connected:
                try:
                    db_history_col.delete_many({})
                    print("Wiped MongoDB history")
                except Exception as e:
                    print(f"Failed to wipe MongoDB: {e}")
            
            # Clear Local History Backup
            with local_history_lock:
                local_history.clear()
                try:
                    with open(ACTIVITY_FILE, 'w', encoding='utf-8') as f:
                        json.dump([], f)
                except Exception:
                    pass
                    
            broadcast('clear', {})
            self.wfile.write(json.dumps({"status": "cleared"}).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

# Keyboard Input listener
def on_press(key):
    global active_buffer, last_typed_time
    
    char = None
    try:
        if hasattr(key, 'char') and key.char is not None:
            char = key.char
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
                broadcast('live', {"text": active_buffer})
        elif char is not None:
            if char == "\n":
                if active_buffer.strip():
                    save_sentence(active_buffer)
                    active_buffer = ""
                    broadcast('live', {"text": ""})
            else:
                active_buffer += char
                broadcast('live', {"text": active_buffer})
                
                if char in ['.', '?', '!']:
                    threading.Thread(target=commit_after_short_delay, args=(active_buffer,)).start()
                    active_buffer = ""

def commit_after_short_delay(text_to_commit):
    time.sleep(0.3)
    save_sentence(text_to_commit)

class ThreadedHTTPServer(ThreadingTCPServer):
    allow_reuse_address = True

def main():
    threading.Thread(target=check_idle_loop, daemon=True).start()
    
    listener = keyboard.Listener(on_press=on_press)
    listener.start()
    
    server = ThreadedHTTPServer(('localhost', PORT), SSEHandler)
    print(f"Real-Time Stream Server on http://localhost:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        listener.stop()
        server.shutdown()
        sys.exit(0)

if __name__ == '__main__':
    main()
