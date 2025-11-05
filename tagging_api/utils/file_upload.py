import os
import sys
import subprocess
import shutil
from loggers.exception import CustomException
from configs.config import S3_FILE_STORAGE, S3_BUCKET_NAME, ALLOWED_FILE_EXTENSIONS
from services.s3_utils import S3PutObject
from loggers.logger import logging
from fastapi import UploadFile


async def file_conv(doc_path):
    base_dir = os.path.dirname(doc_path)
    pdf_path = os.path.splitext(doc_path)[0] + ".pdf"
    libreoffice_path = r"C:\Program Files\LibreOffice\program\soffice.exe"
    
    try:
        logging.info("Starting file conversion using LibreOffice.")
        subprocess.run([
            libreoffice_path,
            "--headless",
            "--convert-to", "pdf",
            "--outdir", base_dir,
            doc_path
        ], check=True)
        logging.info("File conversion done!!!")

        if not os.path.exists(pdf_path):
            raise CustomException("Converted PDF file not found after conversion.", sys)
        return pdf_path
    except subprocess.CalledProcessError as e:
        raise CustomException(f"File Conversion failed: {str(e)}", sys)


async def upload_doc_content(file: UploadFile):
    try:
        extension = os.path.splitext(file.filename)[1].lower()
        converted_filepath = None

        if extension in ALLOWED_FILE_EXTENSIONS:
            logging.info("File is in allowed extensions, converting to PDF.")

            temp_dir = "temp"
            os.makedirs(temp_dir, exist_ok=True)
            doc_path = os.path.join(temp_dir, file.filename)

            file_content = await file.read()
            with open(doc_path, "wb") as f:
                f.write(file_content)

            S3PutObject(data=file_content, bucket_name=S3_BUCKET_NAME, obj_key=f"{S3_FILE_STORAGE}/{file.filename}")

            pdf_filepath = await file_conv(doc_path)

            with open(pdf_filepath, "rb") as doc:
                pdf_content = doc.read()

            converted_filepath = f"{S3_FILE_STORAGE}/{os.path.basename(pdf_filepath)}"
            S3PutObject(data=pdf_content, bucket_name=S3_BUCKET_NAME, obj_key=converted_filepath)

            shutil.rmtree(temp_dir, ignore_errors=True)

            return converted_filepath
        else:
            logging.warning("File extension not allowed.")
            return None

    except Exception as e:
        raise CustomException(f"File upload failed: {str(e)}", sys)
