/**
 * db.js - Simple IndexedDB Wrapper for TypeCraft Stream
 * Safely persists history of sentences typed by the user locally.
 */

const DB_NAME = 'TypeCraftStreamDB';
const DB_VERSION = 1;
const STORE_NAME = 'history';

export const db = {
  dbPromise: null,

  open() {
    if (this.dbPromise) return this.dbPromise;

    this.dbPromise = new Promise((resolve, reject) => {
      const request = indexedDB.open(DB_NAME, DB_VERSION);

      request.onupgradeneeded = (event) => {
        const d = event.target.result;
        if (!d.objectStoreNames.contains(STORE_NAME)) {
          d.createObjectStore(STORE_NAME, { keyPath: 'timestamp' });
        }
      };

      request.onsuccess = (event) => {
        resolve(event.target.result);
      };

      request.onerror = (event) => {
        reject(event.target.error);
      };
    });

    return this.dbPromise;
  },

  async saveRecord(record) {
    const d = await this.open();
    return new Promise((resolve, reject) => {
      const transaction = d.transaction(STORE_NAME, 'readwrite');
      const store = transaction.objectStore(STORE_NAME);
      const request = store.put(record);

      request.onsuccess = () => resolve();
      request.onerror = (e) => reject(e.target.error);
    });
  },

  async getAllRecords() {
    const d = await this.open();
    return new Promise((resolve, reject) => {
      const transaction = d.transaction(STORE_NAME, 'readonly');
      const store = transaction.objectStore(STORE_NAME);
      const request = store.getAll();

      request.onsuccess = (e) => {
        // Sort descending (newest first)
        const sorted = (e.target.result || []).sort((a, b) => b.timestamp - a.timestamp);
        resolve(sorted);
      };
      request.onerror = (e) => reject(e.target.error);
    });
  },

  async clearAll() {
    const d = await this.open();
    return new Promise((resolve, reject) => {
      const transaction = d.transaction(STORE_NAME, 'readwrite');
      const store = transaction.objectStore(STORE_NAME);
      const request = store.clear();

      request.onsuccess = () => resolve();
      request.onerror = (e) => reject(e.target.error);
    });
  }
};
