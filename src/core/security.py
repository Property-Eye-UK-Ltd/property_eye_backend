"""
Security utilities for authentication.
"""

from datetime import datetime, timedelta
from typing import Any, Union

import bcrypt
from jose import jwt

from src.core.config import settings


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against a hash."""
    try:
        if isinstance(plain_password, str):
            plain_password_bytes = plain_password.encode('utf-8')
        else:
            plain_password_bytes = plain_password
            
        if isinstance(hashed_password, str):
            hashed_password_bytes = hashed_password.encode('utf-8')
        else:
            hashed_password_bytes = hashed_password
            
        return bcrypt.checkpw(plain_password_bytes, hashed_password_bytes)
    except (ValueError, TypeError):
        # Handle invalid salt or malformed hash
        return False


def get_password_hash(password: str) -> str:
    """Generate a password hash."""
    if isinstance(password, str):
        password_bytes = password.encode('utf-8')
    else:
        password_bytes = password
        
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password_bytes, salt).decode('utf-8')


def create_access_token(subject: Union[str, Any], expires_delta: timedelta = None) -> str: # type: ignore
    """Create a JWT access token."""
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(
            minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES
        )
    
    to_encode = {"sub": str(subject), "exp": expire}
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt
