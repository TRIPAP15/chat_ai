from pydantic import BaseModel

class FetchDoc(BaseModel):
    user_id: str

class DeleteDoc(BaseModel):
    doc_id: str

class DocExtraction(BaseModel):
    user_id: str
    doc_id: str