from pydantic_settings import BaseSettings
from typing import List
from dotenv import load_dotenv
import os

load_dotenv()

class AppInfo(BaseSettings):
    PROJECT_NAME: str = "Content Effectiveness"
    VERSION: str = "1.0.0"
    DESCRIPTION: str = "API for Content Effectiveness"
    API_V1_STR: str = "/api/v1"
    ALLOWED_ORIGINS: List[str] = ["*"]


ALLOWED_IMAGE_TYPES = [
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/vnd.ms-powerpoint"]

ALLOWED_FILE_EXTENSIONS = ['.doc', '.docx', '.ppt', '.pptx']

DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
AWS_REGION = os.getenv("AWS_REGION")
DATABASE_NAME = os.getenv("DATABASE_NAME")
USER_COLLECTION_NAME = os.getenv("USER_COLLECTION_NAME")
META_COLLECTION_NAME = os.getenv("META_COLLECTION_NAME")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_FILE_STORAGE = "ContentEffectiveness/Uploaded_files"
S3_OUTPUT_STORAGE = "ContentEffectiveness/Extracted_Content/"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
FILE_EXPIRATION_TIME = 86400  # 24 hours
TAG_COLLECTION_NAME = os.getenv("TAG_COLLECTION_NAME")
class Config:
    def __init__(self):
        self.openai_api_key = OPENAI_API_KEY
        self.mongo_uri = DB_CONNECTION_STRING
        self.database_name = DATABASE_NAME
        self.collection_name = TAG_COLLECTION_NAME
        
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required. Please set it in your .env file or system environment variables.")
# Singleton pattern for config
_config_instance = None
def get_config() -> Config:
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance