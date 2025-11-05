from langchain.chains.summarize import load_summarize_chain
from langchain_community.chat_models import ChatOpenAI
from langchain.schema import Document
from configs.config import OPENAI_API_KEY
from loggers.logger import logging

async def text_summarization(text: str) -> str:
    try:
        if not text.strip():
            return "Summary not available."

        docs = [Document(page_content=text)]
        llm = ChatOpenAI(
            model="gpt-4o",
            temperature=0.5,
            openai_api_key=OPENAI_API_KEY
        )
        chain = load_summarize_chain(llm, chain_type="stuff")

        try:
            result = chain.run(docs)
            return result.strip()
        except Exception as e:
            logging.error(f"Error in text summarization: {e}")
            return "Summary not available."
    except Exception as e:
        logging.error(f"Error in text summarization: {e}")
        return "Summary not available."

