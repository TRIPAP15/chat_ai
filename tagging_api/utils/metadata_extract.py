from datetime import datetime
import os
import re
import fitz   # PyMUPDF
import sys
from docling.datamodel.base_models import InputFormat
from concurrent.futures import ThreadPoolExecutor
from docling.document_converter import DocumentConverter, PdfFormatOption
from multiprocessing import Manager
from loggers.logger import logging
from loggers.exception import CustomException
from collections import defaultdict
from statistics import mean
from utils.summary_gen import text_summarization
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode

def filesize_mb(value):
    return round(value / (1024 * 1024), 2)

def remove_img_tag(text):
    try:
        return text.replace('<!-- image -->',"")
    except Exception as e:
        raise CustomException(e, sys)

async def extract_metadata_granular(pdf_document):
    try:
        if pdf_document != None:
            text_properties = []
            for page_num in range(len(pdf_document)):
                page = pdf_document.load_page(page_num)
                text_blocks = page.get_text("dict")
                for block in text_blocks.get("blocks",[]):
                    for line in block.get("lines",[]):
                        for span in line.get("spans",[]):
                            try:
                                font_size = span["size"]
                                font_color = span["color"]
                                font_style = span["font"]
                                text = span["text"]
                                text_properties.append({
                                    "font_size": font_size,
                                    "font_color": font_color,
                                    "font_style": font_style
                                })
                            except:pass

            properties = {
                'font_size': {},
                'font_color': {},
                'font_style': {}
            }

            for prop in text_properties:
                properties['font_size'][prop['font_size']] = properties['font_size'].get(prop['font_size'], 0) + 1
                properties['font_color'][prop['font_color']] = properties['font_color'].get(prop['font_color'], 0) + 1
                properties['font_style'][prop['font_style']] = properties['font_style'].get(prop['font_style'], 0) + 1

            return properties
    except Exception as e:
        raise CustomException(e, sys)

def calculate_distribution_percentage(value):
    if value != {}:
        try:
            total = sum(value.values())

            font_style_percent = {
                font: round((count / total) * 100, 2)
                for font, count in value.items()
            }
            return font_style_percent
        except Exception as e:
            raise CustomException(e, sys)
    else:
        return {}

def clean_document_date(pdf_date_str):
    if pdf_date_str.startswith("D:") and pdf_date_str != "":
        pdf_date_str = pdf_date_str[2:]
    try:
        dt = datetime.strptime(pdf_date_str[:14], "%Y%m%d%H%M%S")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return ""

def process_page_for_dpi(page):
    try:
        images = page.get_images(full=True)
        if not images:
            return []

        dpis = []

        for img in images:
            xref = img[0]
            pix = fitz.Pixmap(page.parent, xref)

            if pix.n > 4:
                pix = fitz.Pixmap(fitz.csRGB, pix)

            dpi_x = pix.width / (page.rect.width / 72)
            dpi_y = pix.height / (page.rect.height / 72)
            avg_dpi = (dpi_x + dpi_y) / 2

            dpis.append(avg_dpi)

        return dpis
    except Exception as e:
        return []

async def get_overall_dpi(pdf_path):
    try:
        doc = fitz.open(pdf_path)

        with ThreadPoolExecutor() as executor:
            results = executor.map(process_page_for_dpi, doc)

        all_dpis = [dpi for dpi_list in results for dpi in dpi_list]
        doc.close()

        if not all_dpis:
            return None

        return round(mean(all_dpis))
    except Exception as e:
        raise CustomException(e, sys)

async def words_paragraphs_lines_extract(result, page_count):
    try:
        num_lines = 0
        num_words = 0
        num_paras = 0
        for page_no in range(1, result.document.num_pages() + 1):
            texts = result.document.export_to_markdown(page_no=page_no)
            num_lines += len(re.split(r'\n\n|\n', texts.strip()))
            num_paras += len(texts.split('\n\n'))
            num_words += len(texts.split())
        return num_lines, num_words, num_paras
    except Exception as e:
        raise CustomException(e, sys)

async def extract_metadata(pdf_path):
    try:
        pdf_document = fitz.open(pdf_path)
        pdf_metadata_overview = pdf_document.metadata
        pdf_metadata_granular = await extract_metadata_granular(pdf_document)
        metadata = {**pdf_metadata_overview,
                    **pdf_metadata_granular}
        metadata['font_size'] = {f"{k:.2f}": v for k, v in metadata['font_size'].items()}
        metadata['creationDate'] = clean_document_date(metadata['creationDate'])
        metadata['font_size_count'] = len(metadata['font_size'].keys())
        metadata['font_color_count'] = len(metadata['font_color'].keys())
        metadata['font_style_count'] = len(metadata['font_style'].keys())
        metadata['font_color'] = {f"#{k:06X}": v for k, v in metadata['font_color'].items()}
        metadata['font_color'] = calculate_distribution_percentage(metadata['font_color'])
        metadata['font_size'] = calculate_distribution_percentage(metadata['font_size'])
        metadata['font_style'] = calculate_distribution_percentage(metadata['font_style'])

        logging.info("Metadata(pymupdf) is extracted from document.")

        pdf_document.close()

        return metadata
    except Exception as e:
        logging.info("Failed to extract metadata.")
        raise CustomException(e, sys)

async def extract_metadata_docling(pdf_path):
    try:
        pipeline_options = PdfPipelineOptions(do_table_structure=True)
        pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE

        doc_converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
        )

        result = doc_converter.convert(pdf_path)
        data = result.model_dump()

        page_picture_count = defaultdict(int)
        page_table_count = defaultdict(int)
        total_pictures = 0
        total_tables = 0
        for picture in data['document']['pictures']:
            if picture.get('label') == 'picture':
                page_no = picture['prov'][0]['page_no']
                page_picture_count[page_no] += 1
                total_pictures += 1
                
        for picture in data['document']['tables']:
            if picture.get('label') == 'table':
                page_no = picture['prov'][0]['page_no']
                page_table_count[page_no] += 1
                total_tables += 1
        
        output = {"pages": {}}
        for page in range(1, data['input']['page_count']+1):
            output["pages"][f"page_{page}"] = { 
                "figures": page_picture_count.get(page, 0),
                "tables": page_table_count.get(page, 0),
                "text": remove_img_tag(result.document.export_to_markdown(page_no=page))
            }
        output["figure_count"] = total_pictures
        output["table_count"] = total_tables
        output['page_count'] = data['input']['page_count']
        output['filesize'] = filesize_mb(data['input']['filesize'])
        output['text'] = remove_img_tag(result.document.export_to_markdown())
        output['summary'] = await text_summarization(result.document.export_to_text())
        output['lines'], output['words'], output['paragraphs'] = await words_paragraphs_lines_extract(result, output['page_count'])

        return output
    except Exception as e:
        logging.info("Failed to extract metadata from Docling.")
        raise CustomException(e, sys)