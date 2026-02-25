from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from src.integrations.alto.client import alto_api_client
from src.integrations.alto.schemas import AltoProperty

router = APIRouter()

@router.get("/properties", response_model=dict)
async def list_alto_properties(
    branch_id: Optional[str] = None,
    status: Optional[str] = None,
    page: int = 1,
    page_size: int = 20
):
    """
    Internal endpoint to test fetching properties from Alto.
    """
    try:
        return await alto_api_client.list_properties(
            branch_id=branch_id,
            status=status,
            page=page,
            page_size=page_size
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/properties/{property_id}", response_model=AltoProperty)
async def get_alto_property(property_id: str):
    """
    Internal endpoint to test fetching a single property from Alto.
    """
    try:
        return await alto_api_client.get_property(property_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
