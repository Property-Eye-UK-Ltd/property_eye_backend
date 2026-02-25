import time
import logging
import httpx
import asyncio
import base64
from typing import Optional
from src.core.config import settings

logger = logging.getLogger(__name__)


def generate_basic_auth_token(client_id: str, client_secret: str) -> str:
    """
    Generates the Basic Authorization token by concatenating Client ID and Client Secret
    separated by a colon, and then Base64 encoding the result.
    """
    credentials = f"{client_id}:{client_secret}"
    return base64.b64encode(credentials.encode()).decode("utf-8")


class AltoAuthClient:
    """
    Client for handling Alto (Zoopla) OAuth2 authentication.

    Manages access tokens, including caching and automatic refreshing.
    """

    def __init__(self):
        self.settings = settings
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0
        self._lock = asyncio.Lock()

    async def get_access_token(self) -> str:
        """
        Returns a valid access token.
        Refreshes the token if it's missing or expired.
        """
        if self._is_token_valid():
            return self._access_token

        async with self._lock:
            # Double-check inside lock
            if self._is_token_valid():
                return self._access_token

            return await self._refresh_token()

    def _is_token_valid(self) -> bool:
        """Checks if the current token is present and not expired (with a buffer)."""
        if not self._access_token:
            return False

        # Add a 60-second buffer to ensure token doesn't expire mid-request
        return time.time() < (self._token_expires_at - 60)

    async def _refresh_token(self) -> str:
        """
        Fetches a new access token from Alto's OAuth2 endpoint.
        """
        logger.info("Refreshing Alto access token...")

        if not self.settings.ALTO_CLIENT_ID or not self.settings.ALTO_CLIENT_SECRET:
            raise ValueError("Alto CLIENT_ID and CLIENT_SECRET must be configured.")

        try:
            basic_auth = generate_basic_auth_token(
                self.settings.ALTO_CLIENT_ID, self.settings.ALTO_CLIENT_SECRET
            )

            async with httpx.AsyncClient() as client:
                # Using client_credentials flow with Basic Auth
                response = await client.post(
                    self.settings.alto_auth_url,
                    data={
                        "grant_type": "client_credentials",
                    },
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Authorization": f"Basic {basic_auth}",
                    },
                )

                if response.status_code != 200:
                    logger.error(
                        f"Failed to authenticate with Alto: {response.status_code} - {response.text}"
                    )
                    response.raise_for_status()

                data = response.json()
                self._access_token = data["access_token"]
                expires_in = data.get(
                    "expires_in", 3600
                )  # Default to 1 hour if not provided
                self._token_expires_at = time.time() + expires_in

                logger.info(
                    f"Successfully acquired Alto access token. Expires in {expires_in} seconds."
                )
                return self._access_token

        except httpx.HTTPError as e:
            logger.error(f"HTTP error during Alto authentication: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during Alto authentication: {str(e)}")
            raise


# Global instance
alto_auth_client = AltoAuthClient()
