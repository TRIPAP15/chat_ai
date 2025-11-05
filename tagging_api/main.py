from fastapi import FastAPI
from configs.config import AppInfo
from fastapi.middleware.cors import CORSMiddleware
from routes.upload_file import upload_router
from routes.router import extraction_router
from routes.tagging_routers import router as tagging_router

def create_application() -> FastAPI:
    info = AppInfo()
    
    application = FastAPI(
        title=info.PROJECT_NAME,
        version=info.VERSION,
        description=info.DESCRIPTION,
        openapi_url=f"{info.API_V1_STR}/vectoriser/openapi.json"
    )
    
    application.add_middleware(
        CORSMiddleware,
        allow_origins=info.ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    application.include_router(upload_router, prefix=info.API_V1_STR)
    application.include_router(extraction_router,prefix=info.API_V1_STR)
    application.include_router(tagging_router, prefix=info.API_V1_STR)
    return application

app = create_application()