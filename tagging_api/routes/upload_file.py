import os
from fastapi import APIRouter, Form, HTTPException, UploadFile, status, File
from configs.config import ALLOWED_IMAGE_TYPES, S3_FILE_STORAGE,S3_BUCKET_NAME, USER_COLLECTION_NAME, ALLOWED_FILE_EXTENSIONS
from services.s3_utils import S3PutObject, S3DeleteObject, S3UploadFile
from db.crud import connect_db
from bson import ObjectId
from models.model import FetchDoc, DeleteDoc
from utils.fetch_doc import generate_presigned_url
from utils.file_upload import upload_doc_content


upload_router = APIRouter()


@upload_router.post('/upload')
async def document_upload(user_id : str = None, file: UploadFile = File(...)):   # str = Form(...)

    user_id = "abc123"
    try:
        if file.content_type not in ALLOWED_IMAGE_TYPES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid file type. Only PDF, DOC and PPT are allowed.")
        
        s3_key = f"{S3_FILE_STORAGE}/{file.filename}"
        conv_filepath = None
        if os.path.splitext(file.filename)[1] in ALLOWED_FILE_EXTENSIONS:
            converted_filepath = await upload_doc_content(file)
            conv_filepath = f"s3://{S3_BUCKET_NAME}/{converted_filepath}"
        else:
            S3PutObject(data = file.file, bucket_name= S3_BUCKET_NAME, obj_key= s3_key)

        schema = {
            "user_id": user_id,
            "filename": file.filename,
            "filepath": f"s3://{S3_BUCKET_NAME}/{s3_key}",
            "converted_filepath": conv_filepath
        }

        id_ = await connect_db.create_doc(collection_name= USER_COLLECTION_NAME,data= schema)
        schema['_id'] = id_
        return schema
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"S3 upload failed: {e}")


@upload_router.post('/fetch-documents')
async def fetch_all_doc(request: FetchDoc):
    try:
        docs = await connect_db.read_doc(collection_name=USER_COLLECTION_NAME,filter_criteria={"user_id":request.user_id})
        return docs
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"S3 upload failed: {e}")



@upload_router.post('/delete-documents')
async def remove_doc(request: DeleteDoc):
    try:
        doc = await connect_db.read_one(USER_COLLECTION_NAME,filter_criteria={"_id": ObjectId(request.doc_id)})

        result = S3DeleteObject(doc['filepath'])

        if result['status'] == "success":
            await connect_db.delete_doc(USER_COLLECTION_NAME, str(doc['_id']))

        return {
            "status": "Success"
        }
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_417_EXPECTATION_FAILED, detail=f"S3 upload failed: {e}")



@upload_router.post('/preview-document')
async def preview_document(doc_id: str):
    if not doc_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Filepath is required for preview.")
    try:
        filter_criteria = {"_id": ObjectId(doc_id)}
        doc = await connect_db.read_doc(USER_COLLECTION_NAME, filter_criteria)
        if not doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found.")
        
        if doc[0]['converted_filepath'] != None:
            s3_key = doc[0]['converted_filepath'].replace(f"s3://{S3_BUCKET_NAME}/", "")
        else:
            s3_key = doc[0]['filepath'].replace(f"s3://{S3_BUCKET_NAME}/", "")
        presigned_url = await generate_presigned_url(s3_key)
        if not presigned_url:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to generate pre-signed URL.")
        return {"url": presigned_url}
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error generating pre-signed URL: {e}")