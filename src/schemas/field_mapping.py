"""
Field mapping schema for document upload.

Defines the structure for mapping agency document columns to system fields.
"""

from typing import Dict

from pydantic import BaseModel, Field


class FieldMappingSchema(BaseModel):
    """
    Schema for field mapping dictionary.

    Maps agency document column names to system-required field names.

    Example:
        {
            "Property Address": "address",
            "Client Full Name": "client_name",
            "Status": "status",
            "Date Withdrawn": "withdrawn_date",
            "Postcode": "postcode"
        }
    """

    mapping: Dict[str, str] = Field(
        ...,
        description="Dictionary mapping agency column names to system field names",
        example={
            "Property Address": "address",
            "Client Full Name": "client_name",
            "Status": "status",
            "Date Withdrawn": "withdrawn_date",
            "Postcode": "postcode",
        },
    )

    class Config:
        json_schema_extra = {
            "example": {
                "mapping": {
                    "Property Address": "address",
                    "Client Full Name": "client_name",
                    "Status": "status",
                    "Date Withdrawn": "withdrawn_date",
                    "Postcode": "postcode",
                }
            }
        }
