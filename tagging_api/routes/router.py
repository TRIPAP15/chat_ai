from fastapi import APIRouter, HTTPException, status
from components.extraction import document_extraction
from models.model import DocExtraction


extraction_router = APIRouter()

@extraction_router.post('/extraction')
async def extract_document(request: DocExtraction):
    try:
        metadata = await document_extraction(request.user_id, request.doc_id)

        response = {
            "page_count": metadata['metadata']['page_count'],
            "paragraphs": metadata['metadata']['paragraphs'],
            "words": metadata['metadata']['words'],
            "font_style_count": metadata['metadata']['font_style_count'],
            "figures_count": metadata['metadata']['figure_count'],
            "table_count": metadata['metadata']['table_count'],
            "title": metadata['metadata']['title'],
            "author": metadata['metadata']['author'],
            "creation_date": metadata['metadata']['creationDate'],
            "filesize": metadata['metadata']['filesize'],
            "file_format": metadata['metadata']['format'],
            "creator": metadata['metadata']['creator'],
            "producer": metadata['metadata']['producer'],
            "language": metadata['metadata']['language'],
            "resolution": metadata['metadata']['resolution'],
            "color_space": metadata['metadata']['color_space'],
            "encryption": metadata['metadata']['encryption'],
            "font_distribution": metadata['metadata']['font_style'],
            "color_distribution": metadata['metadata']['font_color'],
            "summary": metadata['summary'],
            "pages_review": metadata['pages']
        }

        return response
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Extraction failed: {str(e)}")
