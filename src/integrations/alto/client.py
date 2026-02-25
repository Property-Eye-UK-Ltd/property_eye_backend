import logging
import httpx
from typing import Optional, Dict, Any
from src.core.config import settings
from src.integrations.alto.auth import alto_auth_client
from src.integrations.alto.schemas import AltoProperty

logger = logging.getLogger(__name__)


class AltoApiClient:
    """
    Client for interacting with Alto (Zoopla) APIs.
    """

    def __init__(self, auth_client=alto_auth_client):
        self.settings = settings
        self.auth_client = auth_client

    async def _get_headers(
        self, alto_agency_ref: Optional[str] = None
    ) -> Dict[str, str]:
        """Constructs headers with the Bearer token and optional AgencyRef."""
        token = await self.auth_client.get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        if self.settings.ALTO_ENV == "production":
            if not alto_agency_ref:
                raise ValueError(
                    "Configuration Error: alto_agency_ref is required for Alto calls in Production environment."
                )
            # Assuming 'AgencyRef' is the header key based on documentation context.
            # Adjust if specific Zoopla spec requires a different key (e.g. 'X-AgencyRef').
            headers["AgencyRef"] = alto_agency_ref

        return headers

    async def list_properties(
        self,
        alto_agency_ref: Optional[str] = None,
        branch_id: Optional[str] = None,
        status: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:
        """
        Fetches a list of properties from Alto using the /listing/filter endpoint.

        Args:
            alto_agency_ref: Agency reference for production environment.
            branch_id: Optional branch ID to filter by (param: branchId).
            status: Optional status to filter by (param: status).
            page: Page number (param: pageNumber).
            page_size: Number of results per page (param: pageSize).

        Returns:
            JSON response from the API containing the list of properties.
        """
        url = f"{self.settings.alto_api_base_url}/listing/filter"

        # Parameters as per Alto /listing/filter documentation
        params: Dict[str, Any] = {
            "pageNumber": page,
            "pageSize": page_size,
        }

        if branch_id is not None:
            params["branchId"] = branch_id
        if status is not None:
            params["status"] = status

        # Add other filters as needed

        try:
            # Pass alto_agency_ref to _get_headers to handle prod logic
            headers = await self._get_headers(alto_agency_ref)
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=headers, params=params)

                if response.status_code != 200:
                    logger.error(
                        f"Error fetching properties: {response.status_code} - {response.text}"
                    )
                    response.raise_for_status()

                return response.json()

        except httpx.HTTPError as e:
            logger.error(f"HTTP error in list_properties: {str(e)}")
            raise

    async def get_property(
        self, property_id: str, alto_agency_ref: Optional[str] = None
    ) -> AltoProperty:
        """
        Fetches a single property by ID using the /listing/property/{propertyId} endpoint.
        """
        url = f"{self.settings.alto_api_base_url}/listing/property/{property_id}"

        try:
            headers = await self._get_headers(alto_agency_ref)
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=headers)

                if response.status_code == 404:
                    # Handle not found gracefully if needed, or let it raise
                    pass

                if response.status_code != 200:
                    logger.error(
                        f"Error fetching property {property_id}: {response.status_code} - {response.text}"
                    )
                    response.raise_for_status()

                data = response.json()
                return AltoProperty(**data)

        except httpx.HTTPError as e:
            logger.error(f"HTTP error in get_property: {str(e)}")
            raise


# Global instance
alto_api_client = AltoApiClient()
