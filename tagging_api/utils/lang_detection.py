import sys
import re
import fitz  # PyMuPDF
from langdetect import detect, DetectorFactory
from db.languages import LANGDETECT_LANGUAGE_CODES
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
from loggers.logger import logging
from loggers.exception import CustomException

DetectorFactory.seed = 0

def clean_text(text: str) -> str:
    """Replace newlines and multiple spaces with a single space and strip leading/trailing spaces."""
    try:
        unwanted_chars = [
            '\u200B', '\u200C', '\u200D', '\u200E', '\u200F',
            '\u2060', '\u2063', '\uFEFF', '\u00A0', '\u2002',
            '\u2003', '\u2009', '\u202F', '\u3000',
            '\u202A', '\u202B', '\u202C', '\u202D', '\u202E'
        ]
        
        for ch in unwanted_chars:
            text = text.replace(ch, '')
        text = text.replace("\n", " ")
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    except Exception as e:
        raise CustomException(e, sys)

class LanguageDetector():

    def detect_language_text(self, text: str) -> str:
        """Detect language using langdetect"""
        try:
            return detect(text)
        except:
            return "unknown"


    def extract_text_from_page_fitz(self, args):
        """Worker for extracting and detecting language from PyMuPDF text"""
        try:
            page_number, text = args
            if len(text.strip()) < 20:
                return (page_number, "", "fitz", False)
            lang = self.detect_language_text(text)
            return (page_number, lang, "fitz", True)
        except Exception as e:
            raise CustomException(e, sys)

    async def detect_languages(self, pdf_path: str) -> dict:
        """Main detection function with multiprocessing"""
        
        try:
            logging.info("language detection initiated.")
            doc = fitz.open(pdf_path)
            page_texts = [(i, clean_text(page.get_text())) for i, page in enumerate(doc)]

            fitz_results = []
            with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
                futures = [executor.submit(self.extract_text_from_page_fitz, page) for page in page_texts]
                
                for future in as_completed(futures):
                    fitz_results.append(future.result())
            logging.info("Language detection under process...")
            fallback_pages = [i for i, _, _, has_text in fitz_results if not has_text]
            logging.info(f"Page remaining: {len(fallback_pages)}")

            langdetect_langs = []

            for page_num, lang, method, valid in fitz_results:
                if not valid:
                    continue
                if method == "fitz":
                    langdetect_langs.append(lang)
            
            lang_codes = list(set(langdetect_langs))
            lang_codes = [item for item in lang_codes if item != 'unknown']
            languages = [LANGDETECT_LANGUAGE_CODES.get(code) for code in lang_codes]
            if languages == [] and lang_codes == []:
                languages.append("English")
                lang_codes.append('en')
            logging.info("Language detection complete.")
            doc.close()

            return {
                "lang_codes": lang_codes,
                "document_language": languages
            }
        except Exception as e:
            logging.info(f"Language detection failed with error: {e}")
            return CustomException(e, sys)


language_detector = LanguageDetector()