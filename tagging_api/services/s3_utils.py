import json
import boto3
import sys
from configs.config import AWS_REGION
from urllib.parse import urlparse
from loggers.exception import CustomException
from loggers.logger import logging
s3 = boto3.client("s3", region_name=AWS_REGION)

def S3PutObject(bucket_name, obj_key, data, type_of_data='other'):
    try:
        if type_of_data == 'json':
            bytestream_obj = bytes(json.dumps(data).encode('UTF-8'))
            s3.put_object(Bucket=bucket_name, Key=obj_key, Body=bytestream_obj)
            logging.info("Document uploaded into S3.")
        elif type_of_data == 'text':
            ByteStream_obj = bytes(data.encode('UTF-8'))
            s3.put_object(Bucket=bucket_name, Key=obj_key, Body=ByteStream_obj)
            logging.info("Document uploaded into S3.")
        elif type_of_data in ['pdf', 'docx', 'pptx', 'other']:
            s3.put_object(Bucket=bucket_name, Key=obj_key, Body=data)
            logging.info("Document uploaded into S3.")
    except Exception as e:
        raise CustomException(e, sys)
    
def S3UploadFile(data, bucket_name, s3_key):
    try:
        s3.upload_file(data, bucket_name,s3_key,ExtraArgs={'ContentType': 'application/pdf'})
    except Exception as e:
        raise CustomException(e, sys)

def S3DeleteObject(s3_url):
    try:
        filepath = urlparse(s3_url)
        bucket = filepath.netloc
        obj_key = filepath.path.lstrip('/')
        s3.delete_object(Bucket=bucket, Key=obj_key)
        logging.info("Document deleted from S3.")
        return {"status": "success", "message": "File Deleted."}
    except Exception as e:
        raise CustomException(e, sys)
    
def S3DownloadObject(bucket_name, key, local_path):
    try:
        s3.download_file(bucket_name, key, local_path)
        logging.info("Document downloaded from S3.")
        return "success"
    except Exception as e:
        print(f"Download failed!!\n{bucket_name=}\n{key}\n{local_path=}An error occurred: {e}")
        raise CustomException(e, sys)


