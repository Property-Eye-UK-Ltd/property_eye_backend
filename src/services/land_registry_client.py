"""
Land Registry Online Owner Verification (OOV) client.

Handles SOAP + mutual TLS communication with HM Land Registry Business Gateway
for Online Owner Verification, and exposes a simple ownership verification API
for the rest of the application.
"""

import logging
import re
import socket
import ssl
import uuid
from typing import Optional
from urllib.parse import urlparse
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
        parsed_base = urlparse(base_url)
        self._bg_host = parsed_base.hostname or ""

        self._username = config.HMLR_BG_USERNAME
        self._password = config.HMLR_BG_PASSWORD
        self._timeout = config.HMLR_TIMEOUT_SECONDS
        self._cert_path = config.HMLR_TLS_CERT_PATH
        self._key_path = config.HMLR_TLS_KEY_PATH
        self._ca_bundle_path = config.HMLR_CA_BUNDLE_PATH or None

        # OOV endpoint differs between test and production:
        #   test:       /b2b/EOOV_StubService/OnlineOwnershipVerificationV1_0WebService
        #   production: /b2b/EOOV_SoapEngine/OnlineOwnershipVerificationV1_0WebService
        is_test = "bgtest" in base_url
        self._is_test_mode = is_test
        stub_or_engine = "EOOV_StubService" if is_test else "EOOV_SoapEngine"
        self._oov_path = f"/b2b/{stub_or_engine}/OnlineOwnershipVerificationV1_0WebService"

        # Build an explicit SSL context for mutual TLS and CA verification.
        # HMLR_CA_BUNDLE_PATH must be set — system CAs do not include the HMLR
        # root CA, so omitting it will always produce CERTIFICATE_VERIFY_FAILED.
        if not self._ca_bundle_path:
            raise RuntimeError("HMLR_CA_BUNDLE_PATH is not configured. ")

        ssl_context = ssl.create_default_context(cafile=self._ca_bundle_path)
        ssl_context.load_cert_chain(
            certfile=self._cert_path,
            keyfile=self._key_path,
        )

        self.client = httpx.AsyncClient(
            base_url=base_url,
            verify=ssl_context,
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
            if self._bg_host and not self._can_resolve_hostname(self._bg_host):
                logger.error(
                    "Cannot resolve HMLR Business Gateway host '%s' (base_url=%s)",
                    self._bg_host,
                    self.client.base_url,
                )
                return OovResponse(
                    external_reference=request.external_reference,
                    status_code="bg.dns.error",
                    status_message=(
                        f"Cannot resolve hostname '{self._bg_host}'. "
                        "Check DNS/egress from the running environment or use the "
                        "correct HMLR endpoint for that environment."
                    ),
                    matches=[],
                    raw_status_code=0,
                    raw_body=None,
                )

            if request.address:
                addr = request.address
                logger.info(
                    "OOV address fields: building=%s street=%s town=%s postcode=%s",
                    addr.building_name_or_number,
                    addr.street or "",
                    addr.town or "",
                    addr.postcode or "",
                )

            logger.info(
                "HMLR OOV POST %s ref=%s soap_bytes=%s",
                self._oov_path,
                request.external_reference,
                len(soap_body.encode("utf-8")),
            )
            response = await self.client.post(
                self._oov_path,
                content=soap_body.encode("utf-8"),
                headers={"Content-Type": "text/xml; charset=utf-8"},
            )
            logger.info(
                "HMLR OOV HTTP status=%s ref=%s response_bytes=%s",
                response.status_code,
                request.external_reference,
                len(response.content or b""),
            )
            if response.status_code >= 400:
                body_preview = (response.text or "").replace("\n", " ").strip()
                if len(body_preview) > 800:
                    body_preview = f"{body_preview[:800]}…"
                logger.warning(
                    "HMLR OOV HTTP error status=%s ref=%s body=%s",
                    response.status_code,
                    request.external_reference,
                    body_preview or "<empty>",
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
            if self._is_name_resolution_error(exc):
                host = self._bg_host or "<unknown>"
                return OovResponse(
                    external_reference=request.external_reference,
                    status_code="bg.dns.error",
                    status_message=(
                        f"Cannot resolve hostname '{host}' ({exc}). "
                        "Check DNS/egress from the running environment."
                    ),
                    matches=[],
                    raw_status_code=0,
                    raw_body=None,
                )
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
        message_id: Optional[str] = None,
        title_number: Optional[str] = None,
        town: Optional[str] = None,
        building_name_or_number: Optional[str] = None,
    ) -> OwnershipVerificationResult:
        """
        High-level ownership verification using OOV behind the scenes.

        Pass title_number when known (OOV SubjectProperty by title); otherwise
        property_address + postcode (+ optional town for CityName) are used.
        """
        use_title = bool(title_number and str(title_number).strip())
        logger.info(
            "OOV verify_ownership mode=%s postcode_present=%s town_present=%s building_present=%s address_len=%s",
            "title_number" if use_title else "address",
            bool((postcode or "").strip()),
            bool((town or "").strip()),
            bool((building_name_or_number or "").strip()),
            len((property_address or "").strip()),
        )

        # Enforce Land Registry input criteria only outside test mode.
        precheck_error = self._validate_request_prechecks(
            property_address=property_address,
            postcode=postcode,
            title_number=title_number if use_title else None,
        )
        if precheck_error and not self._is_test_mode:
            logger.warning(
                "OOV precheck rejected request: %s - %s",
                precheck_error["status_code"],
                precheck_error["status_message"],
            )
            return OwnershipVerificationResult(
                owner_name=None,
                verification_status="error",
                error_message=f"{precheck_error['status_code']}: {precheck_error['status_message']}",
                raw_response=precheck_error,
            )

        # Build a minimal OOV request from the flat address and expected owner name.
        clean_addr = (property_address or "").strip().rstrip(",").strip()

        # Extract building and street parts. Prefer explicit building name/number if provided.
        if building_name_or_number and building_name_or_number.strip():
            building_part = building_name_or_number.strip()
            # If the building part is already at the start of the address string, 
            # remove it to extract only the street part.
            if clean_addr.upper().startswith(building_part.upper()):
                # Remove building part + optional comma/space
                street_part = re.sub(
                    f"^{re.escape(building_part)}\\s*,?\\s*", 
                    "", clean_addr, flags=re.IGNORECASE
                ).strip()
            else:
                street_part = clean_addr
        else:
            building_part, street_part = self._split_address(clean_addr)
            
        person_forename, person_surname = self._split_name(expected_owner_name)

        if not (person_forename and person_surname) and not self._is_test_mode:
            return OwnershipVerificationResult(
                owner_name=None,
                verification_status="error",
                error_message=(
                    "bg.name.invalid: FirstForename and Surname are required "
                    "for Online Owner Verification requests"
                ),
                raw_response={
                    "status_code": "bg.name.invalid",
                    "status_message": "Missing owner forename/surname",
                },
            )

        # Ensure town is within schema length limits
        final_town = town.strip()[:35] if town and town.strip() else None

        address = OovAddress(
            building_name_or_number=building_part,
            street=street_part,
            town=final_town,
            postcode=self._normalise_uk_postcode(postcode) or None,
        )

        if message_id:
            # Sanitise: Reference must be 1-25 chars, pattern [a-zA-Z0-9][a-zA-Z0-9\-]*
            safe_id = re.sub(r"[^a-zA-Z0-9\-]", "-", message_id)[:25]
            # Strip leading non-alnum chars
            safe_id = re.sub(r"^[^a-zA-Z0-9]+", "", safe_id)
            if not safe_id:
                safe_id = str(uuid.uuid4()).replace("-", "")[:22]
            ref = safe_id[:25]
        else:
            short_id = str(uuid.uuid4()).replace("-", "")[:22]
            ref = f"PE{short_id}"[:25]

        clean_title = str(title_number).strip().upper() if use_title else None

        oov_request = OovRequest(
            external_reference=ref,
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
            title_number=clean_title,
            historical_match=True,
            partial_match=True,
            highlight_additional_owners=True,
        )

        try:
            oov_response = await self.verify_owner(oov_request)
            logger.info(
                "OOV SOAP outcome ref=%s code=%s matches=%s",
                ref,
                oov_response.status_code,
                len(oov_response.matches),
            )
        except Exception as exc:
            logger.error("Unexpected error during OOV verification: %s", exc)
            return OwnershipVerificationResult(
                owner_name=None,
                verification_status="error",
                error_message=str(exc),
                raw_response=None,
            )

        # True API/infrastructure failures: rejection, timeout, parse error, etc.
        # These are codes where we couldn't complete verification at all.
        # bg.match.found and bg.novalidmatch both indicate the API worked — just
        # different name-match outcomes — so they fall through to the match logic.
        _non_match_error_codes = {
            "bg.soap.fault",
            "bg.timeout",
            "bg.request.error",
            "bg.dns.error",
            "bg.parse.error",
            "bg.response.missing",
            "bg.unknown",
        }
        if oov_response.status_code in _non_match_error_codes or (
            oov_response.status_code.startswith("bg.")
            and oov_response.status_code not in {"bg.match.found", "bg.novalidmatch"}
            and "rejection" not in oov_response.status_code
            and oov_response.status_code not in {"bg.properties.nopropertyfound"}
        ):
            logger.warning(
                "HMLR OOV verification failed ref=%s code=%s message=%s raw_status=%s",
                oov_response.external_reference,
                oov_response.status_code,
                oov_response.status_message,
                oov_response.raw_status_code,
            )
            return OwnershipVerificationResult(
                owner_name=None,
                verification_status="error",
                error_message=f"{oov_response.status_code}: {oov_response.status_message}",
                raw_response=oov_response.model_dump(),
            )

        # TypeCode 20 rejections: HMLR explicitly could not find the property.
        # This is a legitimate "cannot verify" result — treat as error, not not_fraud.
        if oov_response.status_code.startswith("bg.properties.") or (
            oov_response.status_code == "bg.rejection"
        ):
            logger.warning(
                "HMLR OOV property rejection ref=%s code=%s message=%s raw_status=%s",
                oov_response.external_reference,
                oov_response.status_code,
                oov_response.status_message,
                oov_response.raw_status_code,
            )
            return OwnershipVerificationResult(
                owner_name=None,
                verification_status="error",
                error_message=f"{oov_response.status_code}: {oov_response.status_message}",
                raw_response=oov_response.model_dump(),
            )

        # TypeCode 30: API completed successfully. Determine match outcome.
        any_match = any(
            o.is_current_owner or o.is_historical_owner
            for t in oov_response.matches
            for o in t.owners
        )
        owner_name = expected_owner_name if any_match else None

        return OwnershipVerificationResult(
            owner_name=owner_name,
            verification_status="ok" if any_match else "not_fraud",
            error_message=None,
            raw_response=oov_response.model_dump(),
        )

    def _validate_request_prechecks(
        self,
        property_address: str,
        postcode: str,
        title_number: Optional[str] = None,
    ) -> Optional[dict]:
        """Validate key LR business-rule criteria before sending live requests."""
        clean_postcode = self._normalise_uk_postcode(postcode)
        clean_address = (property_address or "").strip()

        # BRL-ISBG-011: validate title number format when provided.
        if title_number:
            if not self._is_valid_title_number(title_number):
                return {
                    "status_code": "bg.title.invalid",
                    "status_message": "Title number is invalid",
                    "message_id": "MSG-BG-010",
                    "business_rule": "BRL-ISBG-011",
                }
            return None

        # BRL-ISBG-002: postcode must be syntactically valid.
        if not self._is_valid_uk_postcode(clean_postcode):
            return {
                "status_code": "bg.postcode.invalid",
                "status_message": "Please provide valid postcode",
                "message_id": "MSG-BG-004",
                "business_rule": "BRL-ISBG-002",
            }

        # BRL-ISBG-081: minimum address details check.
        building_part, street_part = self._split_address(clean_address)
        has_building = bool(building_part)
        has_street = bool(street_part)
        has_postcode = bool(clean_postcode)
        has_city = self._looks_like_city_present(clean_address)
        valid_address = has_building and (has_postcode or (has_street and has_city))
        if not valid_address:
            return {
                "status_code": "bg.address.invalidaddresscriteria",
                "status_message": (
                    "Insufficient address details. Please provide house name or number "
                    "and postcode OR house name or number, street and city"
                ),
                "message_id": "MSG-BG-136",
                "business_rule": "BRL-ISBG-081",
            }

        return None

    def _is_valid_uk_postcode(self, postcode: str) -> bool:
        """Check whether a postcode matches UK postcode syntax."""
        postcode_pattern = (
            r"^(GIR 0AA|"
            r"((([A-Z]{1,2}[0-9][A-Z0-9]?)|"
            r"(([A-Z]{1,2}[0-9]{2})))[ ]?[0-9][A-Z]{2}))$"
        )
        return bool(re.match(postcode_pattern, postcode.strip().upper()))

    def _is_valid_title_number(self, title_number: str) -> bool:
        """Check title number syntax against the OOV request schema pattern."""
        candidate = title_number.strip().upper()
        return bool(re.match(r"^(?:[A-Y]{0,3}\d{1,6}|Z\d{1,6}Z)$", candidate))

    def _normalise_uk_postcode(self, postcode: str) -> str:
        """Normalise postcode to uppercase with a single inward-code separator space."""
        compact = re.sub(r"\s+", "", (postcode or "").upper())
        if len(compact) > 3:
            return f"{compact[:-3]} {compact[-3:]}"
        return compact

    def _normalise_name_for_oov(self, value: Optional[str], allow_spaces: bool) -> str:
        """Sanitise name values to characters accepted by the OOV request schema."""
        candidate = (value or "").strip()
        if not candidate:
            return ""
        if allow_spaces:
            cleaned = re.sub(r"[^A-Za-z0-9\-\s']", "", candidate)
            cleaned = re.sub(r"\s+", " ", cleaned).strip()
        else:
            cleaned = re.sub(r"[^A-Za-z0-9\-']", "", candidate)
        return cleaned

    def _looks_like_city_present(self, full_address: str) -> bool:
        """Detect whether a free-form address appears to include a town/city."""
        # Simple heuristic: comma-separated address with at least 3 components
        # usually includes town/city near the end.
        parts = [p.strip() for p in full_address.split(",") if p.strip()]
        return len(parts) >= 3

    def _can_resolve_hostname(self, hostname: str) -> bool:
        """Return True when the target hostname can be resolved in this environment."""
        try:
            socket.getaddrinfo(hostname, 443)
            return True
        except OSError:
            return False

    def _is_name_resolution_error(self, exc: Exception) -> bool:
        """Detect request failures caused by DNS name resolution."""
        current: Optional[BaseException] = exc
        while current:
            if isinstance(current, socket.gaierror):
                return True
            current = current.__cause__ or current.__context__
        message = str(exc).lower()
        return "name or service not known" in message or "temporary failure in name resolution" in message

    async def close(self) -> None:
        """Close the underlying HTTP client connection."""
        await self.client.aclose()

    def _build_oov_request_xml(self, request: OovRequest) -> str:
        """Build SOAP envelope for RequestOnlineOwnershipVerificationV1_0.

        Authentication is via WS-Security UsernameToken in the SOAP header —
        not HTTP Basic auth. Credentials and locale are mandatory per the BG
        developer guide (section 4.3).

        Element ordering inside RequestOOV must follow the XSD sequence:
        MessageId → Reference → SubjectProperty → FirstForename → [MiddleName]
        → Surname → Indicators.
        """
        message_id = xml_escape(request.external_reference)
        reference = xml_escape(request.external_reference)

        # WS-Security UsernameToken header per HMLR OOV Interface Spec.
        # - No mustUnderstand (causes soap:MustUnderstand fault on HMLR BG)
        # - No Timestamp, Nonce, or Created (not expected by this endpoint)
        # - Only Username + Password (PasswordText) inside UsernameToken
        pw_type = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText"
        username_xml = xml_escape(self._username or "")
        password_xml = xml_escape(self._password or "")
        print("---" * 30)
        print('*** Building WS-Security header with username "%s" and password %s', username_xml, password_xml)
        
        print("---" * 30)
        wsse_header = (
            "<wsse:Security>"
            "<wsse:UsernameToken>"
            f"<wsse:Username>{username_xml}</wsse:Username>"
            f'<wsse:Password Type="{pw_type}">{password_xml}</wsse:Password>'
            "</wsse:UsernameToken>"
            "</wsse:Security>"
            '<i18n:international xmlns:i18n="http://www.w3.org/2005/09/ws-i18n">'
            "<i18n:locale>en</i18n:locale>"
            "</i18n:international>"
        )

        # SubjectProperty: title number or property address (title preferred).
        if request.title_number:
            subject_xml = (
                "<req:SubjectProperty>"
                f"<req:TitleNumber>{xml_escape(request.title_number)}</req:TitleNumber>"
                "</req:SubjectProperty>"
            )
        elif request.address:
            addr = request.address
            # BuildingNumber vs BuildingName: use BuildingNumber for numeric-
            # looking values, BuildingName otherwise.
            building_val = addr.building_name_or_number or ""
            first_token = building_val.split()[0] if building_val.split() else ""
            if (
                first_token
                .rstrip("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
                .isdigit()
            ):
                building_xml = f"<req:BuildingNumber>{xml_escape(building_val[:5])}</req:BuildingNumber>"
            else:
                building_xml = f"<req:BuildingName>{xml_escape(building_val[:50])}</req:BuildingName>"
            street_xml = (
                f"<req:StreetName>{xml_escape(addr.street[:80])}</req:StreetName>"
                if addr.street
                else ""
            )
            city_xml = (
                f"<req:CityName>{xml_escape(addr.town[:35])}</req:CityName>"
                if addr.town
                else ""
            )
            postcode_xml = (
                f"<req:PostcodeZone>{xml_escape(addr.postcode[:8])}</req:PostcodeZone>"
                if addr.postcode
                else ""
            )
            subject_xml = (
                "<req:SubjectProperty>"
                "<req:PropertyAddress>"
                f"{building_xml}{street_xml}{city_xml}{postcode_xml}"
                "</req:PropertyAddress>"
                "</req:SubjectProperty>"
            )
        else:
            raise ValueError("OOV request must include either address or title_number.")

        # FirstForename and Surname are direct children of RequestOOV (not
        # wrapped in a PersonName element). Both are mandatory per the XSD.
        if not request.person_name:
            raise ValueError(
                "OOV request must include person_name with forename and surname."
            )

        first = self._normalise_name_for_oov(
            request.person_name.forename, allow_spaces=False
        )
        surname = self._normalise_name_for_oov(
            request.person_name.surname, allow_spaces=True
        )
        if not first or not surname:
            raise ValueError(
                "OOV person_name must include valid forename and surname values."
            )

        forename_xml = f"<req:FirstForename>{xml_escape(first)}</req:FirstForename>"
        surname_xml = f"<req:Surname>{xml_escape(surname)}</req:Surname>"
        middle_xml = ""

        # Indicators.
        skip_partial = not request.partial_match
        skip_historical = not request.historical_match
        indicators_xml = (
            "<req:Indicators>"
            "<req:Indicator>"
            "<req:IndicatorType>ContinueIfOutOfHours</req:IndicatorType>"
            "<req:IndicatorValue>true</req:IndicatorValue>"
            "</req:Indicator>"
            "<req:Indicator>"
            "<req:IndicatorType>SkipPartialMatching</req:IndicatorType>"
            f"<req:IndicatorValue>{'true' if skip_partial else 'false'}</req:IndicatorValue>"
            "</req:Indicator>"
            "<req:Indicator>"
            "<req:IndicatorType>SkipHistoricalMatching</req:IndicatorType>"
            f"<req:IndicatorValue>{'true' if skip_historical else 'false'}</req:IndicatorValue>"
            "</req:Indicator>"
            "</req:Indicators>"
        )

        # WSDL operation: tns:verifyOwnership / <in> (no namespace) containing
        # the RequestOOV fields directly in the req namespace.
        # tns = http://ownershipv1_0.ws.bg.lr.gov/
        # req = http://www.landregistry.gov.uk/OOV/RequestOnlineOwnershipVerificationV1_0
        req_ns = (
            "http://www.landregistry.gov.uk/OOV/RequestOnlineOwnershipVerificationV1_0"
        )
        tns_ns = "http://ownershipv1_0.ws.bg.lr.gov/"

        body_inner = (
            f"<req:MessageId>{xml_escape(message_id)}</req:MessageId>"
            f"<req:Reference>{reference}</req:Reference>"
            f"{subject_xml}"
            f"{forename_xml}"
            f"{middle_xml}"
            f"{surname_xml}"
            f"{indicators_xml}"
        )

        wsse_ns = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
        req_ns = "http://www.landregistry.gov.uk/OOV/RequestOnlineOwnershipVerificationV1_0"
        tns_ns = "http://ownershipv1_0.ws.bg.lr.gov/"

        return (
            '<?xml version="1.0" encoding="UTF-8"?>'
            f'<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"'
            f' xmlns:wsse="{wsse_ns}"'
            f' xmlns:req="{req_ns}"'
            f' xmlns:tns="{tns_ns}">'
            f"<soapenv:Header>{wsse_header}</soapenv:Header>"
            "<soapenv:Body>"
            "<tns:verifyOwnership>"
            f"<in>{body_inner}</in>"
            "</tns:verifyOwnership>"
            "</soapenv:Body>"
            "</soapenv:Envelope>"
        )

    def _parse_oov_response(
        self,
        response: httpx.Response,
        fallback_reference: Optional[str] = None,
    ) -> OovResponse:
        """Parse SOAP XML response into OovResponse."""
        raw_body = response.text
        raw_status = response.status_code

        try:
            # strip_whitespace + process_namespaces collapses ns prefixes to
            # bare local names, which makes downstream key lookups namespace-agnostic.
            parsed = xmltodict.parse(
                raw_body,
                process_namespaces=True,
                namespaces={
                    "http://schemas.xmlsoap.org/soap/envelope/": "soapenv",
                    "http://ownershipv1_0.ws.bg.lr.gov/": "tns",
                    "http://www.landregistry.gov.uk/OOV/ResponseOnlineOwnershipVerificationV1_0": None,
                },
            )
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

        # Drill down through the SOAP envelope to the response payload.
        # After namespace processing the structure is:
        #   soapenv:Envelope > soapenv:Body > tns:verifyOwnershipResponse > return > {TypeCode, ...}
        def _find_key(d: dict, substring: str) -> Optional[dict]:
            """Return the first value whose key contains substring (case-insensitive)."""
            for k, v in d.items():
                if substring in k.lower() and isinstance(v, dict):
                    return v
            return None

        body = _find_key(parsed, "envelope") or parsed
        body = _find_key(body, "body") or body

        # Check for SOAP Fault before looking for the success response.
        # The BG gateway returns a Fault for schema validation errors (e.g. invalid postcode).
        soap_fault = _find_key(body, "fault")
        if soap_fault:
            fault_string = soap_fault.get("faultstring", "SOAP Fault")
            detail = soap_fault.get("detail") or {}
            # Collect all SchemaException messages from detail.
            schema_exc = detail.get("SchemaException") or detail.get(
                "oov:SchemaException"
            )
            if isinstance(schema_exc, list):
                schema_msg = "; ".join(str(e) for e in schema_exc)
            elif schema_exc:
                schema_msg = str(schema_exc)
            else:
                schema_msg = str(detail) if detail else fault_string
            logger.error("SOAP Fault from HMLR OOV: %s — %s", fault_string, schema_msg)
            return OovResponse(
                external_reference=fallback_reference or "",
                status_code="bg.soap.fault",
                status_message=schema_msg,
                matches=[],
                raw_status_code=raw_status,
                raw_body=raw_body,
            )

        # Unwrap the verifyOwnershipResponse / getResponseResponse operation wrapper.
        body = _find_key(body, "response") or body

        # Unwrap <return> element (holds TypeCode + Result/Rejection/Acknowledgement).
        response_oov: Optional[dict] = body.get("return") or _find_key(body, "return")
        if response_oov is None:
            response_oov = body

        # Log the raw parsed payload so tests can see exactly what HMLR returned.
        import json as _json

        logger.info(
            "HMLR OOV raw response payload:\n%s",
            _json.dumps(response_oov, indent=2, default=str),
        )

        # Validate we reached the TypeCode level.
        if not isinstance(response_oov, dict) or "TypeCode" not in response_oov:
            logger.error("Could not locate ResponseOOV payload in OOV response.")
            return OovResponse(
                external_reference=fallback_reference or "",
                status_code="bg.response.missing",
                status_message="Could not locate ResponseOOV payload in response.",
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
                surname_type = str(surname_match.get("TypeOfMatch", "")).strip()

                forename_match = m.get("ForenameMatchDetails", {}) or {}
                forename_type = str(forename_match.get("TypeOfMatch", "")).strip()

                string_match = m.get("StringMatchDetails", {}) or {}
                string_type = str(string_match.get("TypeOfMatch", "")).strip()

                # Determine whether HMLR considers the name a match.
                # A name is matched when either:
                #   (a) Surname is MATCH or PARTIAL_MATCH (and forename is not NO_MATCH), or
                #   (b) Surname is NO_MATCH but StringMatch is MATCH (full-name string match).
                POSITIVE = {"MATCH", "PARTIAL_MATCH"}
                name_is_match = (
                    surname_type in POSITIVE and forename_type not in {"NO_MATCH"}
                ) or (surname_type == "NO_MATCH" and string_type == "MATCH")

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
                        name_match_type=f"surname:{surname_type} forename:{forename_type}",
                        forename=None,
                        surname=None,
                        company_name=None,
                        is_current_owner=not is_historical and name_is_match,
                        is_historical_owner=is_historical and name_is_match,
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
        
        # Normalize whitespace and strip
        addr = re.sub(r"\s+", " ", full_address.strip())
        
        # 1. Try splitting by comma first (e.g. "Woodstock, Brookfield Lane West")
        if "," in addr:
            parts = [p.strip() for p in addr.split(",", 1)]
            building = parts[0].strip(",")
            street = parts[1].strip(",")
            if building and street:
                return building, street
        
        # 2. Try splitting on first space (e.g. "10 Brookfield Lane West")
        parts = addr.split(" ", 1)
        if len(parts) == 1:
            return parts[0].strip(","), ""
        
        return parts[0].strip(","), parts[1].strip(",")

    def _split_name(self, full_name: str) -> tuple[Optional[str], Optional[str]]:
        """Split a full name into simple forename and surname."""
        if not full_name:
            return None, None
        tokens = full_name.strip().split()
        if len(tokens) == 1:
            return tokens[0], tokens[0]
        return tokens[0], tokens[-1]
