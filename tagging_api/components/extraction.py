import os
import sys
import tempfile
from utils.fetch_doc import fetch_s3_obj_key
from configs.config import S3_OUTPUT_STORAGE, S3_BUCKET_NAME, META_COLLECTION_NAME
from services.s3_utils import S3DownloadObject, S3PutObject
from utils.lang_detection import language_detector
from utils.metadata_extract import extract_metadata, extract_metadata_docling, get_overall_dpi
from loggers.logger import logging
from db.crud import connect_db
from loggers.exception import CustomException


async def document_extraction(user_id, doc_id):
    try:
        obj_key = await fetch_s3_obj_key(user_id, doc_id)
        logging.info("Object key fetched from document id")

        local_dir = tempfile.mkdtemp()  # '/tmp/'
        file_name = os.path.basename(obj_key)
        relative_dir, _ = os.path.splitext(file_name)
        output_dir = os.path.join(S3_OUTPUT_STORAGE, relative_dir)
        pdf_path = os.path.join(local_dir, file_name)
        metadata = {}
        metadata['user_id'] = user_id
        metadata['doc_id'] = doc_id
        S3DownloadObject(S3_BUCKET_NAME, obj_key, pdf_path)
        logging.info(f"Document downloaded in the pdf_path: {pdf_path}")

        pymupdf_result = await extract_metadata(pdf_path)
        metadata['metadata'] = pymupdf_result
        logging.info("Metadata Extraction from PyMuPDF Success!!!")

        language_dict = await language_detector.detect_languages(pdf_path=pdf_path)
        logging.info("Language Detection Success!!!")

        metadata['metadata']['language'] = language_dict['document_language']

        docling_result = await extract_metadata_docling(pdf_path)
        logging.info("Metadata Extraction from Docling Success!!!")

        metadata['metadata']['figure_count'] = docling_result['figure_count']
        metadata['metadata']['table_count'] = docling_result['table_count']
        metadata['text'] = docling_result['text']
        metadata['summary'] = docling_result['summary']
        metadata['pages'] = docling_result['pages']
        metadata['metadata']['filesize'] = docling_result['filesize']
        metadata['metadata']['page_count'] = docling_result['page_count']
        metadata['metadata']['lines'] = docling_result['lines']
        metadata['metadata']['words'] = docling_result['words']
        metadata['metadata']['paragraphs'] = docling_result['paragraphs']
        metadata['metadata']['font_style'] = pymupdf_result['font_style']
        metadata['metadata']['resolution'] = await get_overall_dpi(pdf_path)
        metadata['metadata']['color_space'] = "Hex"
        logging.info("Metadata Collected!!!")

        obj_key = f"{output_dir}/metadata.json"
        S3PutObject(S3_BUCKET_NAME, obj_key, metadata, type_of_data='json')
        logging.info("Metadata uploaded to S3.")

        await connect_db.create_doc(META_COLLECTION_NAME, metadata)
        logging.info("Metadata stored in mongoDB")

        for page in metadata['pages'].values():
            page.pop("text", None)

        return metadata

    except Exception as e:
        raise CustomException(e, sys)

