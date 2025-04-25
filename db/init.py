import os
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


class DB:
    def __init__(self):
        # Get the directory where init.py is located
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Construct the path to private_key.json
        key_path = os.path.join(current_dir, 'private_key.json')
        self.app = firebase_admin.initialize_app(credentials.Certificate(key_path))
        self.db = firestore.client()

    def get_db(self):
        return self.db

    def get_app(self):
        return self.app



