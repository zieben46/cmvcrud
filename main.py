import uvicorn
from fastapi import FastAPI
from app.api.endpoints import TableAPI

import logging

# Configure logging to write to a file
logging.basicConfig(
    filename="app.log",  # Log file name
    level=logging.INFO,  # Set log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("🚀 FastAPI server starting up...")


app = FastAPI(title="FastAPI CRUD API", version="1.0")

# ✅ Include API Endpoints from APIRouter
table_api = TableAPI()
app.include_router(table_api.router)


# ✅ Run the app
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
