import sys
from db.crud import connect_db
from bson import ObjectId
from configs.config import USER_COLLECTION_NAME
from loggers.logger import logging
from loggers.exception import CustomException
import boto3
from botocore.config import Config
from configs.config import AWS_REGION, S3_BUCKET_NAME, FILE_EXPIRATION_TIME

s3 = boto3.client("s3", region_name=AWS_REGION)

async def fetch_s3_obj_key(user_id: str,doc_id: str):
    try:
        filter_criteria = {"user_id":user_id, "_id": ObjectId(doc_id)}

        doc = await connect_db.read_one(USER_COLLECTION_NAME,filter_criteria)
        if doc['converted_filepath'] == None:
            obj_key = doc['filepath'].replace("s3://pharma-ai-suite/","")
        else:
            obj_key = doc['converted_filepath'].replace("s3://pharma-ai-suite/","")

        return obj_key
    except Exception as e:
        raise CustomException(e, sys)
    
async def generate_presigned_url(s3_key: str):
    try:
        url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': S3_BUCKET_NAME,
            'Key': s3_key,
            'ResponseContentDisposition': 'inline',
            'ResponseContentType': 'application/pdf'
            },
            ExpiresIn=FILE_EXPIRATION_TIME, HttpMethod='GET')
        return url
    except Exception as e:
        print(f"Error generating pre-signed URL: {e}")
        return None