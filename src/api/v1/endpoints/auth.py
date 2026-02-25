"""
Authentication endpoints.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api import deps
from src.core import security
from src.db.session import get_db
from src.models.agency import Agency
from src.schemas.agency import AgencyCreate, AgencyLogin, Token, AgencyResponse

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/signup", response_model=AgencyResponse, status_code=status.HTTP_201_CREATED)
async def signup(agency_in: AgencyCreate, db: AsyncSession = Depends(get_db)):
    """
    Create a new agency account.
    """
    # Check if username exists
    stmt = select(Agency).where(Agency.username == agency_in.username)
    result = await db.execute(stmt)
    if result.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered",
        )

    # Create new agency
    agency = Agency(
        name=agency_in.name,
        username=agency_in.username,
        hashed_password=security.get_password_hash(agency_in.password),
    )
    db.add(agency)
    await db.commit()
    await db.refresh(agency)
    return agency


@router.post("/login", response_model=Token)
async def login(login_data: AgencyLogin, db: AsyncSession = Depends(get_db)):
    """
    Login and get access token.
    """
    # Authenticate user
    stmt = select(Agency).where(Agency.username == login_data.username)
    result = await db.execute(stmt)
    agency = result.scalars().first()

    if not agency or not security.verify_password(login_data.password, agency.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Create access token
    access_token = security.create_access_token(subject=agency.id)
    
    # Store token in DB
    agency.access_token = access_token
    await db.commit()

    return {
        "access_token": access_token,
        "token_type": "bearer",
        "agency_name": agency.name,
        "agency_id": agency.id
    }


@router.post("/logout")
async def logout(
    current_agency: Agency = Depends(deps.get_current_agency),
    db: AsyncSession = Depends(get_db)
):
    """
    Logout current agency (invalidate token).
    """
    current_agency.access_token = None
    await db.commit()
    return {"message": "Successfully logged out"}


@router.get("/me", response_model=AgencyResponse)
async def read_users_me(current_agency: Agency = Depends(deps.get_current_agency)):
    """
    Get current agency details.
    """
    return current_agency
