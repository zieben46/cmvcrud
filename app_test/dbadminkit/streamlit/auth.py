import hashlib
from typing import Dict, Optional

# Dummy user store with roles (replace with database later)
USERS: Dict[str, Dict[str, str]] = {
    "admin": {"password": hashlib.sha256("admin123".encode()).hexdigest(), "role": "admin"},
    "user1": {"password": hashlib.sha256("user123".encode()).hexdigest(), "role": "user"},
}

def authenticate_user(username: str, password: str) -> Optional[dict]:
    """Authenticate user and return their info if valid."""
    if username in USERS:
        hashed_password = hashlib.sha256(password.encode()).hexdigest()
        if USERS[username]["password"] == hashed_password:
            return {"username": username, "role": USERS[username]["role"]}
    return None