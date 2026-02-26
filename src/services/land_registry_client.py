"""
Land Registry Online Owner Verification (OOV) client.

Handles SOAP + mutual TLS communication with HM Land Registry Business Gateway
for Online Owner Verification, and exposes a simple ownership verification API
for the rest of the application.
"""

import logging
import uuid
from typing import Optional
from xml.sax.saxutils import escape as xml_escape

import httpx
import xmltodict

from src.schemas.land_registry_oov import (
    OovRequest,
    OovResponse,
    OovAddress,
    OovMatchedTitle,
    OovOwner,
)
from src.utils.constants import config

logger = logging.getLogger(__name__)


class OwnershipVerificationResult:
    """Result of high-level ownership verification using OOV."""

    def __init__(
        self,
        owner_name: Optional[str] = None,
        verification_status: str = "error",
        error_message: Optional[str] = None,
        raw_response: Optional[dict] = None,
    ):
        self.owner_name = owner_name
        self.verification_status = verification_status
        self.error_message = error_message
        self.raw_response = raw_response


class LandRegistryClient:
    """
    Client for HM Land Registry Online Owner Verification (OOV).

    Provides methods for building SOAP requests, sending them over mutual TLS,
    parsing XML responses into Pydantic models, and exposing a simplified
    ownership verification API for Stage 2 checks.
    """

    def __init__(self) -> None:
        """Initialize OOV SOAP client using Business Gateway configuration."""
        base_url = (config.HMLR_BG_BASE_URL or "").rstrip("/")
        if not base_url:
            logger.warning("HMLR_BG_BASE_URL is not configured; OOV calls will fail.")

        self._username = config.HMLR_BG_USERNAME
        self._password = config.HMLR_BG_PASSWORD
        self._timeout = config.HMLR_TIMEOUT_SECONDS
        self._cert = (config.HMLR_TLS_CERT_PATH, config.HMLR_TLS_KEY_PATH)

        # The OOV SOAP path is fixed for this integration; do not rely on env.
        self._oov_path = (
            "/b2b/EOOV_SoapEngine/OnlineOwnershipVerificationV1_0WebService"
        )
        print("cert", self._cert)

        # attempt to log out parts of the cert to verify correct file path
        with open(self._cert[0], "r") as f:
            print("cert 0", f.read())
        with open(self._cert[1], "r") as f:
            print("cert 1", f.read())

        self.client = httpx.AsyncClient(
            base_url=base_url,
            cert=self._cert,
            verify=True,
            timeout=self._timeout,
        )

    async def verify_owner(self, request: OovRequest) -> OovResponse:
        """
        Call Online Owner Verification with a structured OovRequest.
        """
        if not request.address and not request.title_number:
            raise ValueError("OOV request must include either address or title_number.")

        soap_body = self._build_oov_request_xml(request)

        try:
            response = await self.client.post(
                self._oov_path,
                content=soap_body,
                headers={"Content-Type": "text/xml; charset=utf-8"},
                auth=(self._username, self._password),
            )
        except httpx.TimeoutException as exc:
            logger.error("Timeout calling HMLR OOV service: %s", exc)
            return OovResponse(
                external_reference=request.external_reference,
                status_code="bg.timeout",
                status_message="HMLR OOV request timed out",
                matches=[],
                raw_status_code=0,
                raw_body=None,
            )
        except httpx.RequestError as exc:
            logger.error("Request error calling HMLR OOV service: %s", exc)
            return OovResponse(
                external_reference=request.external_reference,
                status_code="bg.request.error",
                status_message=str(exc),
                matches=[],
                raw_status_code=0,
                raw_body=None,
            )

        return self._parse_oov_response(
            response, fallback_reference=request.external_reference
        )

    async def verify_ownership(
        self,
        property_address: str,
        postcode: str,
        expected_owner_name: str,
    ) -> OwnershipVerificationResult:
        """
        High-level ownership verification using OOV behind the scenes.

        This method preserves the existing interface used by VerificationService
        while delegating the actual check to the Online Owner Verification SOAP
        service.
        """
        logger.info(
            "Verifying ownership for %s, %s via OOV", property_address, postcode
        )

        # Build a minimal OOV request from the flat address and expected owner name.
        building_part, street_part = self._split_address(property_address)
        person_forename, person_surname = self._split_name(expected_owner_name)

        address = OovAddress(
            building_name_or_number=building_part,
            street=street_part,
            town=None,
            postcode=postcode or None,
        )

        oov_request = OovRequest(
            external_reference=f"property-eye-{uuid.uuid4()}",
            customer_reference=None,
            person_name=(
                None
                if not (person_forename or person_surname)
                else {
                    "title": None,
                    "forename": person_forename,
                    "surname": person_surname,
                }
            ),
            company_name=None,
            address=address,
            title_number=None,
            historical_match=True,
            partial_match=True,
            highlight_additional_owners=True,
        )

        try:
            oov_response = await self.verify_owner(oov_request)
        except Exception as exc:
            logger.error("Unexpected error during OOV verification: %s", exc)
            return OwnershipVerificationResult(
                owner_name=None,
                verification_status="error",
                error_message=str(exc),
                raw_response=None,
            )

        # Interpret OOV response in simple ok/error terms for existing flows.
        if (
            oov_response.status_code.startswith("bg.")
            and "match" not in oov_response.status_code
        ):
            # Treat explicit Business Gateway error/rejection codes as error.
            return OwnershipVerificationResult(
                owner_name=None,
                verification_status="error",
                error_message=oov_response.status_message,
                raw_response=oov_response.model_dump(),
            )

        any_match = any(t.owners for t in oov_response.matches)
        owner_name = expected_owner_name if any_match else None

        return OwnershipVerificationResult(
            owner_name=owner_name,
            verification_status="ok" if any_match else "error",
            error_message=None if any_match else oov_response.status_message,
            raw_response=oov_response.model_dump(),
        )

    async def close(self) -> None:
        """Close the underlying HTTP client connection."""
        await self.client.aclose()

    def _build_oov_request_xml(self, request: OovRequest) -> str:
        """Build SOAP envelope for RequestOnlineOwnershipVerificationV1_0."""
        message_id = f"PropertyEye-OOV-{uuid.uuid4()}"

        reference = xml_escape(request.external_reference)

        # SubjectProperty: title number or property address (title preferred when present).
        if request.title_number:
            subject_xml = (
                f"<oov:SubjectProperty>"
                f"<oov:TitleNumber>{xml_escape(request.title_number)}</oov:TitleNumber>"
                f"</oov:SubjectProperty>"
            )
        elif request.address:
            addr = request.address
            building_xml = f"<oov:BuildingNumber>{xml_escape(addr.building_name_or_number)}</oov:BuildingNumber>"
            street_xml = (
                f"<oov:StreetName>{xml_escape(addr.street)}</oov:StreetName>"
                if addr.street
                else ""
            )
            city_xml = (
                f"<oov:CityName>{xml_escape(addr.town)}</oov:CityName>"
                if addr.town
                else ""
            )
            postcode_xml = (
                f"<oov:PostcodeZone>{xml_escape(addr.postcode)}</oov:PostcodeZone>"
                if addr.postcode
                else ""
            )
            subject_xml = (
                "<oov:SubjectProperty>"
                "<oov:PropertyAddress>"
                f"{building_xml}{street_xml}{city_xml}{postcode_xml}"
                "</oov:PropertyAddress>"
                "</oov:SubjectProperty>"
            )
        else:
            raise ValueError("OOV request must include either address or title_number.")

        # Name or company – OOV requires FirstForename and Surname for individuals.
        person_name_xml = ""
        if request.person_name:
            first = request.person_name.forename or ""
            middle = ""
            surname = request.person_name.surname or ""
            if request.person_name.title:
                # Title is not part of the schema; we ignore it at XML level.
                logger.debug("Ignoring person title in OOV request XML.")

            if not first or not surname:
                logger.debug(
                    "Partial person name supplied; OOV may reject this request."
                )

            first_xml = f"<oov:FirstForename>{xml_escape(first)}</oov:FirstForename>"
            surname_xml = f"<oov:Surname>{xml_escape(surname)}</oov:Surname>"
            middle_xml = ""
            if middle:
                middle_xml = (
                    "<oov:MiddleName>"
                    f"<oov:MiddleName>{xml_escape(middle)}</oov:MiddleName>"
                    "</oov:MiddleName>"
                )

            person_name_xml = f"{first_xml}{middle_xml}{surname_xml}"

        # Indicators – map our booleans onto SkipPartialMatching / SkipHistoricalMatching.
        indicators: list[str] = []

        # ContinueIfOutOfHours -> always true so that queued requests get processed.
        indicators.append(
            "<oov:Indicator>"
            "<oov:IndicatorType>ContinueIfOutOfHours</oov:IndicatorType>"
            "<oov:IndicatorValue>true</oov:IndicatorValue>"
            "</oov:Indicator>"
        )

        skip_partial = not request.partial_match
        indicators.append(
            "<oov:Indicator>"
            "<oov:IndicatorType>SkipPartialMatching</oov:IndicatorType>"
            f"<oov:IndicatorValue>{'true' if skip_partial else 'false'}</oov:IndicatorValue>"
            "</oov:Indicator>"
        )

        skip_historical = not request.historical_match
        indicators.append(
            "<oov:Indicator>"
            "<oov:IndicatorType>SkipHistoricalMatching</oov:IndicatorType>"
            f"<oov:IndicatorValue>{'true' if skip_historical else 'false'}</oov:IndicatorValue>"
            "</oov:Indicator>"
        )

        indicators_xml = "<oov:Indicators>" + "".join(indicators) + "</oov:Indicators>"

        request_oov_xml = (
            "<oov:RequestOOV>"
            f"<oov:MessageId>{xml_escape(message_id)}</oov:MessageId>"
            f"<oov:Reference>{reference}</oov:Reference>"
            f"{subject_xml}"
            f"{person_name_xml}"
            f"{indicators_xml}"
            "</oov:RequestOOV>"
        )

        envelope = (
            '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
            'xmlns:oov="http://www.landregistry.gov.uk/OOV/RequestOnlineOwnershipVerificationV1_0">'
            "<soapenv:Header/>"
            "<soapenv:Body>"
            f"{request_oov_xml}"
            "</soapenv:Body>"
            "</soapenv:Envelope>"
        )

        return envelope

    def _parse_oov_response(
        self,
        response: httpx.Response,
        fallback_reference: Optional[str] = None,
    ) -> OovResponse:
        """Parse SOAP XML response into OovResponse."""
        raw_body = response.text
        raw_status = response.status_code

        try:
            parsed = xmltodict.parse(raw_body)
        except Exception as exc:
            logger.error("Failed to parse OOV XML response: %s", exc)
            return OovResponse(
                external_reference=fallback_reference or "",
                status_code="bg.parse.error",
                status_message=str(exc),
                matches=[],
                raw_status_code=raw_status,
                raw_body=raw_body,
            )

        # Drill down to ResponseOOV element, handling potential SOAP wrappers.
        body = parsed
        for key in list(parsed.keys()):
            lowered = key.lower()
            if "envelope" in lowered:
                body = parsed[key]
                break

        for key in list(body.keys()):
            lowered = key.lower()
            if "body" in lowered:
                body = body[key]
                break

        response_oov = None
        for key, value in body.items():
            if "responseoov" in key.lower():
                response_oov = value
                break

        if response_oov is None:
            # Handle bare ResponseOOV with no SOAP wrapper.
            for key, value in parsed.items():
                if "responseoov" in key.lower():
                    response_oov = value
                    break

        if response_oov is None:
            logger.error("Could not locate ResponseOOV element in OOV response.")
            return OovResponse(
                external_reference=fallback_reference or "",
                status_code="bg.response.missing",
                status_message="Could not locate ResponseOOV element in response.",
                matches=[],
                raw_status_code=raw_status,
                raw_body=raw_body,
            )

        type_code = str(response_oov.get("TypeCode", "")).strip()
        acknowledgement = response_oov.get("Acknowledgement")
        rejection = response_oov.get("Rejection")
        result = response_oov.get("Result")

        status_code = "bg.unknown"
        status_message: Optional[str] = None
        external_reference = fallback_reference or ""
        matches: list[OovMatchedTitle] = []

        if type_code == "10" and acknowledgement:
            status_code = "bg.acknowledgement"
            status_message = acknowledgement.get("MessageDescription")
        elif type_code == "20" and rejection:
            status_code = rejection.get("Code", "bg.rejection")
            status_message = rejection.get("Reason")
            external_reference = rejection.get("Reference", external_reference)
        elif type_code == "30" and result:
            match_result = result.get("MatchResult", "")
            if match_result == "NO_MATCHES":
                status_code = "bg.novalidmatch"
            else:
                status_code = "bg.match.found"

            status_message = result.get("Message")
            external_reference = result.get("Reference", external_reference)

            raw_matches = result.get("Match") or []
            if isinstance(raw_matches, dict):
                raw_matches = [raw_matches]

            for m in raw_matches:
                subject = m.get("SubjectProperty", {})
                title_number = str(subject.get("TitleNumber", "")).strip()
                prop_addr = subject.get("PropertyAddress", {}) or {}

                building_name = (
                    prop_addr.get("BuildingNumber")
                    or prop_addr.get("BuildingName")
                    or prop_addr.get("SubBuildingName")
                )
                street = prop_addr.get("StreetName")
                city = prop_addr.get("CityName")
                postcode = prop_addr.get("PostcodeZone")

                oov_address = OovAddress(
                    building_name_or_number=building_name or "",
                    street=street,
                    town=city,
                    postcode=postcode,
                )

                owners: list[OovOwner] = []

                surname_match = m.get("SurnameMatch", {}) or {}
                surname_type = surname_match.get("TypeOfMatch", "")

                match_infos = m.get("MatchInformation") or []
                if isinstance(match_infos, dict):
                    match_infos = [match_infos]

                is_historical = False
                for info in match_infos:
                    if (
                        info.get("Name") == "HistoricalMatch"
                        and str(info.get("Value")).lower() == "true"
                    ):
                        is_historical = True
                        break

                owners.append(
                    OovOwner(
                        name_match_type=str(surname_type),
                        forename=None,
                        surname=None,
                        company_name=None,
                        is_current_owner=not is_historical,
                        is_historical_owner=is_historical,
                    )
                )

                matches.append(
                    OovMatchedTitle(
                        title_number=title_number,
                        address=oov_address,
                        owners=owners,
                    )
                )

        return OovResponse(
            external_reference=external_reference,
            status_code=status_code,
            status_message=status_message,
            matches=matches,
            raw_status_code=raw_status,
            raw_body=raw_body,
        )

    def _split_address(self, full_address: str) -> tuple[str, str]:
        """Split a free-form address into building number/name and street."""
        if not full_address:
            return "", ""
        parts = full_address.strip().split(" ", 1)
        if len(parts) == 1:
            return parts[0], ""
        return parts[0], parts[1]

    def _split_name(self, full_name: str) -> tuple[Optional[str], Optional[str]]:
        """Split a full name into simple forename and surname."""
        if not full_name:
            return None, None
        tokens = full_name.strip().split()
        if len(tokens) == 1:
            return tokens[0], tokens[0]
        return tokens[0], tokens[-1]
