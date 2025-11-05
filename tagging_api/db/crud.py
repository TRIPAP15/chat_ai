import pymongo
import sys
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError
from typing import Dict, List, Optional, Any
from contextlib import contextmanager
from bson import ObjectId
from configs.config import DB_CONNECTION_STRING, DATABASE_NAME
from datetime import datetime
from loggers.logger import logging
from loggers.exception import CustomException

class DBUtils:
    """A class to handle CRUD operations for MongoDB with production-level features."""
    
    def __init__(self):
        """
        Initialize MongoDBCRUD with connection details.
        
        Args:
            connection_string (str): MongoDB connection string
            database_name (str): Name of the database to connect to
        """
        self.connection_string = DB_CONNECTION_STRING
        self.database_name = DATABASE_NAME
        self.client = None
        self.db = None
        self._connect()

    def _connect(self) -> None:
        """Establish connection to MongoDB."""
        try:
            self.client = MongoClient(self.connection_string)
            self.db = self.client[self.database_name]

        except ConnectionFailure as e:
            raise CustomException(e, sys)

    @contextmanager
    def _get_collection(self, collection_name: str):
        """Context manager for collection operations."""
        try:
            collection = self.db[collection_name]
            yield collection
        except PyMongoError as e:
            logging.info(f"Collection operation error: {str(e)}")
            raise e
        finally:
            pass

    async def create_doc(self, collection_name: str, data: Dict[str, Any]) -> str:
        """
        Create a new document in the specified collection.
        
        Args:
            collection_name (str): Name of the collection
            data (Dict[str, Any]): Document data to insert
            
        Returns:
            str: ID of the inserted document
        """
        try:
            data['creation_date'] = f"{datetime.now().date()}"
            data['creation_time'] = f"{datetime.now().time()}"
            data['updation_date'] = None
            data['updation_time'] = None
            
            with self._get_collection(collection_name) as collection:
                result = collection.insert_one(data)
                logging.info("Document inserted in DB.")
                return str(result.inserted_id)
        except PyMongoError as e:
            logging.info("Document insertion failed.")
            raise CustomException(e, sys)

    async def read_doc(self, collection_name: str, filter_criteria: Dict[str, Any], ) -> List[Dict[str, Any]]:
        """
        Read documents from the specified collection.
        
        Args:
            collection_name (str): Name of the collection
            query (Dict[str, Any]): Query filter
            
        Returns:
            List[Dict[str, Any]]: List of matching documents
        """
        try:
            with self._get_collection(collection_name) as collection:
                cursor = collection.find(filter_criteria)
                results = [
                    {**doc, '_id': str(doc['_id'])} 
                    for doc in cursor
                ]
                logging.info("Document reading success...")
                return results
        except PyMongoError as e:
            logging.info("Document reading failed.")
            raise e

    async def read_one(self, collection_name: str, filter_criteria: Dict) -> Optional[Dict[str, Any]]:
        """
        Read a single document by ID.
        
        Args:
            collection_name (str): Name of the collection
            document_id (str): Document ID
            
        Returns:
            Optional[Dict[str, Any]]: Matching document or None
        """
        try:
            with self._get_collection(collection_name) as collection:
                doc = collection.find_one(
                    filter_criteria)
                if doc:
                    doc['_id'] = str(doc['_id'])
                    logging.info("documents fetched from DB.")
                    return doc
                logging.info("No document is present.")
                return None
        except PyMongoError as e:
            logging.info("Documents reading failed.")
            raise CustomException(e, sys)

    async def update_doc(self, collection_name: str, document_id: str,
              update_data: Dict[str, Any]) -> bool:
        """
        Update a document in the specified collection.
        
        Args:
            collection_name (str): Name of the collection
            document_id (str): Document ID
            update_data (Dict[str, Any]): Update operations
            
        Returns:
            bool: True if update was successful, False if document not found
        """
        try:
            update_data['updation_date'] = f"{datetime.now().date()}"
            update_data['creation_time'] = f"{datetime.now().time()}"
            update_data['updation_time'] = None
            with self._get_collection(collection_name) as collection:
                result = collection.update_one(
                    {'_id': ObjectId(document_id)},
                    {'$set': update_data}
                )
                if result.matched_count > 0:
                    logging.info(f"document updated. doc_id: {document_id}")
                    return True
                logging.info(f"No document found. doc_id: {document_id}")
                return False
        except PyMongoError as e:
            logging.info("Document updation failed.")
            raise

    async def delete_doc(self, collection_name: str, document_id: str) -> bool:
        """
        Delete a document from the specified collection.
        
        Args:
            collection_name (str): Name of the collection
            document_id (str): Document ID
            
        Returns:
            bool: True if deletion was successful, False if document not found
        """
        try:
            with self._get_collection(collection_name) as collection:
                result = collection.delete_one({'_id': ObjectId(document_id)})
                if result.deleted_count > 0:
                    logging.info(f"document deleted. doc_id: {document_id}")
                    return True
                logging.info(f"No document found. doc_id: {document_id}")
                return False
        except PyMongoError as e:
            logging.info("Document deletion failed.")
            raise

    def close(self) -> None:
        """Close the MongoDB client connection."""
        if self.client:
            self.client.close()
            self.logger.info("MongoDB connection closed")
            self.client = None

connect_db = DBUtils()