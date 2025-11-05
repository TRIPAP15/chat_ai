from typing import Dict, Optional
from datetime import datetime
from bson import ObjectId
from bson.errors import InvalidId
from motor.motor_asyncio import AsyncIOMotorClient
from configs.config import Config

class DocumentService:
    def __init__(self, config: Config):
        self.config = config
        self.mongo_client = AsyncIOMotorClient(config.mongo_uri)
        self.db = self.mongo_client[config.database_name]
        self.collection = self.db[config.collection_name]
    
    def convert_to_object_id(self, document_id: str):
        """Convert string ID to ObjectId if it's a valid ObjectId format, otherwise return as string"""
        try:
            return ObjectId(document_id)
        except InvalidId:
            return document_id

    async def find_document_by_id(self, document_id: str):
        """Find document by ID, trying both ObjectId and string formats"""
        try:
            obj_id = ObjectId(document_id)
            document = await self.collection.find_one({"_id": obj_id})
            if document:
                return document
        except InvalidId:
            pass
        # Try searching by a custom document_id field if it exists
        document = await self.collection.find_one({"doc_id": document_id})
        if document:
            return document
        return None

    async def store_tags_to_mongodb(self, document_id: str, tags: Dict):
        """Store the generated tags back to MongoDB"""
        try:
            update_data = {
                "generated_tags": tags,
                "tags_generated_at": datetime.now().isoformat(),
                "tags_updated_at": datetime.now().isoformat()
            }
            
            try:
                obj_id = ObjectId(document_id)
                result = await self.collection.update_one(
                    {"_id": obj_id}, 
                    {"$set": update_data}
                )
                if result.matched_count > 0:
                    return True
            except InvalidId:
                pass
            # Try with document_id field
            result = await self.collection.update_one(
                {"doc_id": document_id}, 
                {"$set": update_data}
            )
            if result.matched_count > 0:
                return True
            return False
            
        except Exception as e:
            print(f"[ERROR] Failed to store tags to MongoDB: {str(e)}")
            return False
    
    async def get_document_text(self, document_id: str) -> Optional[str]:
        """Get text content from document"""
        document = await self.find_document_by_id(document_id)
        
        if not document:
            return None
        
        if "text" not in document:
            raise ValueError(f"text field not found in document. Available fields: {list(document.keys())}")
        
        text_content = document["text"]
        
        if not isinstance(text_content, str):
            raise ValueError(f"text field must be a string, got {type(text_content)}")
        
        if not text_content.strip():
            raise ValueError("text field is empty")
        
        return text_content