"""
API dependencies.
"""

from typing import Generator, Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.db.session import get_db
from src.models.agency import Agency

oauth2_scheme = OAuth2PasswordBearer(tokenUrl=f"{settings.API_V1_PREFIX}/auth/login")


async def get_current_agency(
    db: AsyncSession = Depends(get_db), token: str = Depends(oauth2_scheme)
) -> Agency:
    """
    Get the current authenticated agency from the JWT token.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        payload = jwt.decode(
            token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM]
        )
        agency_id = payload.get("sub")
        # Validate subject claim type and presence
        if agency_id is None or not isinstance(agency_id, str):
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    stmt = select(Agency).where(Agency.id == agency_id)
    result = await db.execute(stmt)
    agency = result.scalars().first()
    # Ensure we have an ORM instance, not a class/table expression
    if not isinstance(agency, Agency):
        raise credentials_exception
    
    if agency is None:
        raise credentials_exception
        
    # Verify token matches what's in DB (as requested by user)
    # Ensure the token matches the persisted session token to prevent reuse
    token_in_db = agency.access_token  # type: ignore[attr-defined]
    if token_in_db != token: # type: ignore
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session expired or invalid",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return agency
