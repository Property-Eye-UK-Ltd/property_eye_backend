"""
Field mapping schema for document upload.

Defines the structure for mapping agency document columns to system fields.
"""

from typing import Dict

from pydantic import BaseModel, ConfigDict, Field


class FieldMappingSchema(BaseModel):
    """
    Schema for field mapping dictionary.

    Maps agency document column names to system field names.

    Use client_name only when the source column explicitly means the purchaser /
    applicant / buyer (not the vendor). "Client Full Name" in many UK exports is
    the selling client — map that to vendor_name unless your file clearly labels
    the buying party.

    Example:
        {
            "Property Address": "address",
            "Vendor Name": "vendor_name",
            "Applicant Name": "client_name",
            "Status": "status",
            "Date Withdrawn": "withdrawn_date",
            "Postcode": "postcode"
        }
    """

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "mapping": {
                    "Property Address": "address",
                    "Vendor Name": "vendor_name",
                    "Applicant Name": "client_name",
                    "Status": "status",
                    "Date Withdrawn": "withdrawn_date",
                    "Postcode": "postcode",
                }
            }
        }
    )

    mapping: Dict[str, str] = Field(
        ...,
        description="Dictionary mapping agency column names to system field names",
    )
