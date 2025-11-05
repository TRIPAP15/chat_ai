from fastapi import APIRouter, HTTPException, Depends
from datetime import datetime
from models.tagging_models import TaggingRequest, TaggingResponse
from services.tagging_service import TaggingService
from services.document_service import DocumentService
from configs.config import get_config

router = APIRouter(tags=["tagging"])

def get_tagging_service() -> TaggingService:
    config = get_config()
    return TaggingService(config)

def get_document_service() -> DocumentService:
    config = get_config()
    return DocumentService(config)

@router.post("/generate_tags", response_model=TaggingResponse)
async def generate_tags(
    request: TaggingRequest,
    tagging_service: TaggingService = Depends(get_tagging_service),
    document_service: DocumentService = Depends(get_document_service)
):
    """Generate and store abstractive and extractive tags for a document"""
    start_time = datetime.now()
    try:
        text_content = await document_service.get_document_text(request.document_id)
        if not text_content:
            raise HTTPException(
                status_code=404, 
                detail=f"Document not found with ID: {request.document_id}"
            )
        
        tags = await tagging_service.tag_document(
            text_content, 
            request.chunk_size, 
            request.min_extractive_threshold
        )
        
        stored = await document_service.store_tags_to_mongodb(request.document_id, tags)
        processing_time = (datetime.now() - start_time).total_seconds()
        
        return TaggingResponse(
            document_id=request.document_id,
            tags=tags,
            processing_time=processing_time,
            timestamp=datetime.now().isoformat(),
            stored=stored,
            min_extractive_threshold_used=request.min_extractive_threshold
        )
        
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")