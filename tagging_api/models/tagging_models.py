from pydantic import BaseModel, Field
from typing import Dict, Optional, Any

class TaggingRequest(BaseModel):
    document_id: str
    chunk_size: Optional[int] = Field(
        default=5000, 
        ge=100, 
        le=10000, 
        description="Size of text chunks for processing"
    )
    min_extractive_threshold: Optional[float] = Field(
        default=1.0, 
        ge=0.1, 
        le=50.0, 
        description="Minimum percentage score for extractive tags to be included"
    )

class TaggingResponse(BaseModel):
    document_id: str
    tags: Dict[str, Any]
    processing_time: float
    timestamp: str
    stored: bool
    min_extractive_threshold_used: Optional[float] = None