import logging
from datetime import datetime
from firebase_admin import firestore
from .init import DB

class FirebaseStorage:
    def __init__(self):
        self.db = DB().get_db()
        
    def save_checkpoint(self, checkpoint_data):
        """Save checkpoint data to Firebase"""
        try:
            # Store in 'checkpoints' collection with timestamp as document ID
            checkpoint_ref = self.db.collection('checkpoints').document()
            checkpoint_ref.set({
                'timestamp': firestore.SERVER_TIMESTAMP,
                'data': checkpoint_data
            })
            logging.info("Checkpoint saved to Firebase")
            return checkpoint_ref.id
        except Exception as e:
            logging.error(f"Error saving checkpoint to Firebase: {e}")
            return None

    def load_checkpoint(self):
        """Load latest checkpoint from Firebase"""
        try:
            # Get the most recent checkpoint
            checkpoints = (self.db.collection('checkpoints')
                         .order_by('timestamp', direction=firestore.Query.DESCENDING)
                         .limit(1)
                         .stream())
            
            for checkpoint in checkpoints:
                return checkpoint.to_dict()['data']
            return None
        except Exception as e:
            logging.error(f"Error loading checkpoint from Firebase: {e}")
            return None

    def save_analysis_results(self, results):
        """Save analysis results to Firebase"""
        try:
            # Create a new document in 'analysis_results' collection
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            result_ref = self.db.collection('analysis_results').document(timestamp)
            
            # Store the main results
            result_ref.set({
                'timestamp': firestore.SERVER_TIMESTAMP,
                'shorts': results['shorts'],
                'regular_videos': results['regular_videos'],
                'category_stats': results['category_stats']
            })

            logging.info(f"Analysis results saved to Firebase with ID: {timestamp}")
            return timestamp
        except Exception as e:
            logging.error(f"Error saving results to Firebase: {e}")
            return None

    def get_analysis_results(self, result_id=None):
        """Retrieve analysis results from Firebase"""
        try:
            if result_id:
                # Get specific result
                result = self.db.collection('analysis_results').document(result_id).get()
                return result.to_dict() if result.exists else None
            else:
                # Get latest result
                results = (self.db.collection('analysis_results')
                          .order_by('timestamp', direction=firestore.Query.DESCENDING)
                          .limit(1)
                          .stream())
                
                for result in results:
                    return result.to_dict()
                return None
        except Exception as e:
            logging.error(f"Error retrieving results from Firebase: {e}")
            return None 