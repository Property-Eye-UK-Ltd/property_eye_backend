from typing import List, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime

class AltoAddress(BaseModel):
    address_line_1: Optional[str] = None
    address_line_2: Optional[str] = None
    town: Optional[str] = None
    postcode: Optional[str] = None
    country: Optional[str] = None

class AltoPrice(BaseModel):
    amount: Optional[float] = None
    currency_code: Optional[str] = "GBP"

class AltoProperty(BaseModel):
    """
    Represents a property listing from Alto.
    """
    id: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    status: Optional[str] = None  # e.g., "available", "sold", "withdrawn"
    address: Optional[AltoAddress] = None
    price: Optional[AltoPrice] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    summary_description: Optional[str] = None
    branch_id: Optional[str] = None
    
    # Allow extra fields to capture everything from API without validation error
    model_config = {"extra": "allow"}

class AltoPropertyListResponse(BaseModel):
    """
    Response model for property listing endpoint.
    """
    _embedded: Optional[dict] = None # Often HAL format
    properties: List[AltoProperty] = Field(default_factory=list)
    total_count: Optional[int] = 0
    
    # Adjust based on actual API response structure (e.g. if it uses HAL or plain list)
    # For now assuming a simple wrapper or list
    
    model_config = {"extra": "allow"}

class AltoPropertyFilter(BaseModel):
    """
    Filters for querying properties.
    """
    status: Optional[str] = None
    branch_id: Optional[str] = None
    modified_since: Optional[datetime] = None
    include_withdrawn: bool = False
