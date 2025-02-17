import uvicorn
from fastapi import FastAPI, Depends, HTTPException
from app.controllers.api_controller import APIController
from app.auth.auth import get_current_user
from app.auth.auth import create_access_token
from datetime import timedelta

from fastapi import Form

import logging

# Configure logging to write to a file
logging.basicConfig(
    filename="app.log",  # Log file name
    level=logging.INFO,  # Set log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("🚀 FastAPI server starting up...")


app = FastAPI(title="FastAPI CRUD API", version="1.0")

# ✅ Include API Endpoints from APIController
api_controller = APIController()
app.include_router(api_controller.router)

# ✅ Authentication Route (Token Generation)
@app.post("/token")
# def login(username: str, password: str):
def login(username: str = Form(...), password: str = Form(...)):
    """Authenticate user and return JWT token."""
    fake_users_db = {
        "admin": {"username": "admin", "password": "admin123", "role": "admin"},
        "user": {"username": "user", "password": "user123", "role": "editor"},
    }
    user = fake_users_db.get(username)
    if not user or user["password"] != password:
        raise HTTPException(status_code=401, detail="❌ Invalid credentials")
    
    access_token = create_access_token({"sub": username, "role": user["role"]}, expires_delta=timedelta(minutes=30))
    return {"access_token": access_token, "token_type": "bearer"}

# ✅ Protected Route Example
@app.get("/protected-route")
def protected_route(user: dict = Depends(get_current_user)):
    """Example of a protected route using authentication."""
    return {"message": f"🔒 Welcome, {user['username']}! You have `{user['role']}` permissions."}

# ✅ Run the app
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
