"""Register Extract Service integration and caching."""

from __future__ import annotations

import base64
from dataclasses import dataclass
import io
import logging
import json
import re
import uuid
import zipfile
from datetime import datetime
from typing import Any, Optional
from xml.sax.saxutils import escape as xml_escape

import xmltodict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from src.core.config import settings
from src.models.fraud_match import FraudMatch
from src.models.property_listing import PropertyListing
from src.models.register_extract import RegisterExtract
from src.schemas.register_extract import (
    RegisterExtractEntrySchema,
    RegisterExtractPropertySchema,
    RegisterExtractProprietorSchema,
    RegisterExtractResponseSchema,
)
from src.services.land_registry_client import LandRegistryClient

logger = logging.getLogger(__name__)

MOCK_TITLE_NUMBER = "GR506405"
MOCK_PDF_BYTES = base64.b64decode(
    "JVBERi0xLjQKMSAwIG9iago8PCAvVHlwZSAvQ2F0YWxvZyAvUGFnZXMgMiAwIFIgPj4KZW5kb2JqCjIg"
    "MCBvYmoKPDwgL1R5cGUgL1BhZ2VzIC9LaWRzIFszIDAgUl0gL0NvdW50IDEgPj4KZW5kb2JqCjMgMCBv"
    "YmoKPDwgL1R5cGUgL1BhZ2UgL1BhcmVudCAyIDAgUiAvTWVkaWFCb3ggWzAgMCA1OTUgODQyXSAvQ29u"
    "dGVudHMgNCAwIFIgL1Jlc291cmNlcyA8PCAvRm9udCA8PCAvRjEgNSAwIFIgPj4gPj4gPj4KZW5kb2Jq"
    "CjQgMCBvYmoKPDwgL0xlbmd0aCA3MSA+PgpzdHJlYW0KQlQKL0YxIDE4IFRmCjcyIDc1MCBUZAooUHJv"
    "cGVydHkgRXllIFJlZ2lzdGVyIEV4dHJhY3QgTW9jaykgVGoKRVQKZW5kc3RyZWFtCmVuZG9iago1IDAg"
    "b2JqCjw8IC9UeXBlIC9Gb250IC9TdWJ0eXBlIC9UeXBlMSAvQmFzZUZvbnQgL0hlbHZldGljYSA+Pgpl"
    "bmRvYmoKeHJlZgowIDYKMDAwMDAwMDAwMCA2NTUzNSBmIAowMDAwMDAwMDEwIDAwMDAwIG4gCjAwMDAw"
    "MDAwNjAgMDAwMDAgbiAKMDAwMDAwMDExNyAwMDAwMCBuIAowMDAwMDAwMjQyIDAwMDAwIG4gCjAwMDAw"
    "MDAzNjMgMDAwMDAgbiAKdHJhaWxlcgo8PCAvU2l6ZSA2IC9Sb290IDEgMCBSID4+CnN0YXJ0eHJlZgo0"
    "NTQKJSVFT0Y="
)


@dataclass
class TitleDiscoveryResult:
    """Resolved title-number discovery outcome for a register extract request."""

    title_number: str = ""
    source: str = "none"
    message: Optional[str] = None


class RegisterExtractService:
    """Fetch, parse, cache, and return register extracts for fraud cases."""

    async def get_or_fetch(
        self,
        report_id: str,
        db: AsyncSession,
        mock: bool = False,
    ) -> RegisterExtractResponseSchema:
        fraud_match = await self._get_fraud_match(report_id, db)
        cached = fraud_match.register_extract
        logger.info(
            "Register Extract request | report_id=%s | mock=%s | cache_status=%s | cached_title=%s | listing_title=%s | has_oov_response=%s",
            report_id,
            mock,
            cached.status if cached else None,
            cached.title_number if cached else None,
            fraud_match.property_listing.title_number or None,
            bool(fraud_match.land_registry_response),
        )
        if cached and cached.status == "complete" and cached.parsed_json:
            if self._should_refresh_cached_extract(cached.parsed_json, cached.raw_xml):
                logger.info(
                    "Register Extract cache stale | report_id=%s | title_number=%s | refetching=true",
                    report_id,
                    cached.title_number,
                )
            else:
                logger.info(
                    "Register Extract cache hit | report_id=%s | title_number=%s | fetched_at=%s",
                    report_id,
                    cached.title_number,
                    cached.fetched_at,
                )
                return RegisterExtractResponseSchema.model_validate(cached.parsed_json)
        elif cached and cached.status == "complete":
            logger.info(
                "Register Extract cache hit | report_id=%s | title_number=%s | fetched_at=%s",
                report_id,
                cached.title_number,
                cached.fetched_at,
            )
            return RegisterExtractResponseSchema.model_validate(cached.parsed_json)

        if mock:
            logger.info("Register Extract mock mode | report_id=%s", report_id)
            payload = self._build_mock_extract(fraud_match)
            await self._save_extract(
                fraud_match=fraud_match,
                payload=payload,
                raw_xml="<MockRegisterExtract />",
                db=db,
            )
            return payload

        discovery = await self._discover_title_number(fraud_match, db)
        title_number = discovery.title_number
        logger.info(
            "Register Extract title resolution | report_id=%s | resolved_title=%s | source=%s | message=%s",
            report_id,
            title_number or None,
            discovery.source,
            discovery.message,
        )
        if not title_number:
            logger.warning(
                "Register Extract failed before live request | report_id=%s | listing_title=%s | oov_response_present=%s | discovery_message=%s",
                report_id,
                fraud_match.property_listing.title_number or None,
                bool(fraud_match.land_registry_response),
                discovery.message,
            )
            payload = RegisterExtractResponseSchema(
                report_id=fraud_match.id,
                title_number=None,
                fetched_at=datetime.utcnow(),
                status="failed",
                property=RegisterExtractPropertySchema(
                    address=fraud_match.property_listing.address,
                    description=fraud_match.property_listing.address,
                ),
                official_copy_available=False,
                error_message="Title number is required for Register Extract requests.",
            )
            await self._save_extract(
                fraud_match=fraud_match,
                payload=payload,
                raw_xml=None,
                db=db,
            )
            return payload

        result = await self._request_live_extract(
            report_id=fraud_match.id,
            title_number=title_number,
            property_address=fraud_match.property_listing.address,
            vendor_name=fraud_match.property_listing.vendor_name,
            include_pdf=False,
        )
        await self._save_extract(
            fraud_match=fraud_match,
            payload=result["payload"],
            raw_xml=result.get("raw_xml"),
            db=db,
        )
        return result["payload"]

    def _should_refresh_cached_extract(self, payload: Any, raw_xml: Optional[str]) -> bool:
        """Detect stale summary-only cache entries produced before richer parsing."""
        if not isinstance(payload, dict):
            return True

        if isinstance(raw_xml, str) and ("<Fault" in raw_xml or "versionmismatch" in raw_xml.lower()):
            return True

        detail_keys = ("proprietors", "charges", "restrictions", "leases", "notices", "quick_reference_flags")
        if any(payload.get(key) for key in detail_keys):
            return False

        property_block = payload.get("property")
        if isinstance(property_block, dict) and any(
            property_block.get(key) for key in ("tenure", "description")
        ):
            return True

        return True

    async def get_pdf_bytes(
        self,
        report_id: str,
        db: AsyncSession,
        mock: bool = False,
    ) -> tuple[bytes, str]:
        fraud_match = await self._get_fraud_match(report_id, db)
        logger.info(
            "Register Extract PDF request | report_id=%s | mock=%s | cached_title=%s | listing_title=%s | has_oov_response=%s",
            report_id,
            mock,
            fraud_match.register_extract.title_number if fraud_match.register_extract else None,
            fraud_match.property_listing.title_number or None,
            bool(fraud_match.land_registry_response),
        )
        if mock:
            return MOCK_PDF_BYTES, f"register-extract-{MOCK_TITLE_NUMBER}.pdf"

        discovery = await self._discover_title_number(fraud_match, db)
        title_number = discovery.title_number
        logger.info(
            "Register Extract PDF title resolution | report_id=%s | resolved_title=%s | source=%s | message=%s",
            report_id,
            title_number or None,
            discovery.source,
            discovery.message,
        )
        if not title_number:
            raise ValueError("Title number is required to download the official copy.")

        result = await self._request_live_extract(
            report_id=fraud_match.id,
            title_number=title_number,
            property_address=fraud_match.property_listing.address,
            vendor_name=fraud_match.property_listing.vendor_name,
            include_pdf=True,
        )
        pdf_bytes = result.get("pdf_bytes")
        if not pdf_bytes:
            raise ValueError("Register Extract response did not include a downloadable PDF.")
        return pdf_bytes, f"register-extract-{title_number}.pdf"

    async def _get_fraud_match(self, report_id: str, db: AsyncSession) -> FraudMatch:
        stmt = (
            select(FraudMatch)
            .options(
                joinedload(FraudMatch.property_listing).joinedload(PropertyListing.agency),
                joinedload(FraudMatch.register_extract),
            )
            .where(FraudMatch.id == report_id)
        )
        result = await db.execute(stmt)
        fraud_match = result.scalar_one_or_none()
        if not fraud_match:
            raise ValueError(f"Fraud report {report_id} not found")
        return fraud_match

    async def _save_extract(
        self,
        fraud_match: FraudMatch,
        payload: RegisterExtractResponseSchema,
        raw_xml: Optional[str],
        db: AsyncSession,
    ) -> None:
        cached = fraud_match.register_extract or RegisterExtract(fraud_match_id=fraud_match.id)
        if fraud_match.register_extract is None:
            db.add(cached)
        cached.title_number = payload.title_number
        cached.raw_xml = raw_xml
        cached.parsed_json = payload.model_dump(mode="json")
        cached.fetched_at = payload.fetched_at
        cached.status = payload.status
        cached.error_message = payload.error_message
        await db.commit()

    async def _discover_title_number(
        self,
        fraud_match: FraudMatch,
        db: AsyncSession,
    ) -> TitleDiscoveryResult:
        """Resolve and persist a title number from the listing, OOV, or address search."""
        title_number = (fraud_match.property_listing.title_number or "").strip().upper()
        if title_number:
            logger.info(
                "Register Extract title source | report_id=%s | source=listing | title_number=%s",
                fraud_match.id,
                title_number,
            )
            return TitleDiscoveryResult(title_number=title_number, source="listing")

        title_number = self._extract_title_number_from_oov_response(
            fraud_match.land_registry_response
        )
        if title_number:
            logger.info(
                "Register Extract title source | report_id=%s | source=oov_response | title_number=%s",
                fraud_match.id,
                title_number,
            )
            fraud_match.property_listing.title_number = title_number
            await db.commit()
            return TitleDiscoveryResult(title_number=title_number, source="oov_response")

        try:
            search_result = await self._search_title_number_by_address(fraud_match)
        except Exception as exc:
            logger.exception(
                "Register Extract property-description title search crashed | report_id=%s | address=%s",
                fraud_match.id,
                fraud_match.property_listing.address,
            )
            return TitleDiscoveryResult(
                title_number="",
                source="property_description_error",
                message=str(exc),
            )
        search_title = getattr(search_result, "title_number", None) or ""
        if search_title:
            fraud_match.property_listing.title_number = search_title
            await db.commit()
            logger.info(
                "Register Extract title source | report_id=%s | source=property_description | title_number=%s",
                fraud_match.id,
                search_title,
            )
            return TitleDiscoveryResult(
                title_number=search_title,
                source="property_description",
            )

        logger.info(
            "Register Extract title source | report_id=%s | source=none | has_oov_response=%s | search_status=%s",
            fraud_match.id,
            bool(fraud_match.land_registry_response),
            getattr(search_result, "status_code", None),
        )
        return TitleDiscoveryResult(
            title_number="",
            source="none",
            message=getattr(search_result, "status_message", None),
        )

    async def _search_title_number_by_address(
        self,
        fraud_match: FraudMatch,
    ) -> Any:
        """Look up a title number from the property description using HMLR."""
        logger.info(
            "Register Extract property-description search start | report_id=%s | address=%s | postcode=%s",
            fraud_match.id,
            fraud_match.property_listing.address,
            fraud_match.property_listing.postcode or fraud_match.ppd_postcode or None,
        )
        client = LandRegistryClient()
        try:
            result = await client.search_title_by_property_description(
                property_address=fraud_match.property_listing.address,
                postcode=fraud_match.property_listing.postcode or fraud_match.ppd_postcode or "",
                message_id=fraud_match.id,
                customer_reference=fraud_match.property_listing.agency.name
                if fraud_match.property_listing.agency
                else None,
            )
            logger.info(
                "Register Extract property-description search completed | report_id=%s | status=%s | title_number=%s",
                fraud_match.id,
                getattr(result, "status_code", None),
                getattr(result, "title_number", None),
            )
            return result
        finally:
            await client.close()

    def _extract_title_number_from_oov_response(self, raw_response: Optional[str]) -> str:
        """Extract a title number from a stored OOV response payload."""
        if not raw_response:
            logger.info("Register Extract OOV response empty while resolving title number")
            return ""

        try:
            payload = json.loads(raw_response)
        except Exception:
            logger.warning("Register Extract OOV response was not valid JSON")
            return ""

        candidates = [
            payload,
            payload.get("matches") if isinstance(payload, dict) else None,
        ]
        while candidates:
            current = candidates.pop(0)
            if isinstance(current, dict):
                for key, value in current.items():
                    if key.lower() in {"title_number", "titlenumber"} and isinstance(value, str):
                        cleaned = value.strip().upper()
                        if cleaned:
                            logger.info(
                                "Register Extract OOV response title match found | title_number=%s",
                                cleaned,
                            )
                            return cleaned
                    candidates.append(value)
            elif isinstance(current, list):
                candidates.extend(current)
        logger.info("Register Extract OOV response did not contain a title number")
        return ""

    async def _request_live_extract(
        self,
        report_id: str,
        title_number: str,
        property_address: str,
        vendor_name: Optional[str],
        include_pdf: bool,
    ) -> dict[str, Any]:
        client = LandRegistryClient()
        try:
            service_path = settings.HMLR_RES_PATH.strip() or client.oc_with_summary_path

            request_xml = self._build_live_request_xml(
                message_id=self._make_message_id(report_id),
                title_number=title_number,
                property_address=property_address,
            )
            logger.info(
                "Register Extract live request | report_id=%s | service_path=%s | include_pdf=%s | title_number=%s | property_address=%s | request_bytes=%s",
                report_id,
                service_path,
                include_pdf,
                title_number,
                property_address,
                len(request_xml.encode("utf-8")),
            )
            response = await client.client.post(
                service_path,
                content=request_xml.encode("utf-8"),
                headers={"Content-Type": "text/xml; charset=utf-8"},
            )
            preview = (response.text or "").replace("\n", " ").strip()
            if len(preview) > 1000:
                preview = f"{preview[:1000]}…"
            logger.info(
                "Register Extract live response | report_id=%s | status=%s | bytes=%s | preview=%s",
                report_id,
                response.status_code,
                len(response.content or b""),
                preview or "<empty>",
            )
            parsed = self._parse_live_response(
                report_id=report_id,
                title_number=title_number,
                raw_xml=response.text,
                vendor_name=vendor_name,
                property_address=property_address,
            )
            logger.info(
                "Register Extract parsed response | report_id=%s | status=%s | official_copy=%s | proprietor_count=%s | charge_count=%s | restriction_count=%s | lease_count=%s | notice_count=%s",
                report_id,
                parsed["payload"].status,
                parsed["payload"].official_copy_available,
                len(parsed["payload"].proprietors),
                len(parsed["payload"].charges),
                len(parsed["payload"].restrictions),
                len(parsed["payload"].leases),
                len(parsed["payload"].notices),
            )
            attachment_bytes = parsed.pop("attachment_bytes", None)
            pdf_bytes = self._extract_pdf_bytes(attachment_bytes) if include_pdf else None
            return {
                "payload": parsed["payload"],
                "raw_xml": response.text,
                "pdf_bytes": pdf_bytes,
            }
        except Exception as exc:
            logger.error("Register Extract request failed for report %s: %s", report_id, exc)
            payload = RegisterExtractResponseSchema(
                report_id=report_id,
                title_number=title_number,
                fetched_at=datetime.utcnow(),
                status="failed",
                property=RegisterExtractPropertySchema(
                    address=property_address,
                    description=property_address,
                ),
                official_copy_available=False,
                error_message=str(exc),
            )
            return {"payload": payload, "raw_xml": None, "pdf_bytes": None}
        finally:
            await client.close()

    def _build_live_request_xml(
        self,
        message_id: str,
        title_number: str,
        property_address: str,
    ) -> str:
        ref = xml_escape(message_id[:25])
        description = xml_escape(property_address[:130] or title_number)
        title = xml_escape(title_number)
        username = xml_escape(settings.HMLR_BG_USERNAME)
        password = xml_escape(settings.HMLR_BG_PASSWORD)
        i18n_ns = "http://www.w3.org/2005/09/ws-i18n"
        wsse_ns = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
        req_ns = "http://www.oscre.org/ns/eReg-Final/2011/RequestOCWithSummaryV2_0"
        op_ns = "http://ocwithsummaryv2_1.ws.bg.lr.gov/"
        pw_type = (
            "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText"
        )
        return (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
            f'xmlns:wsse="{wsse_ns}" xmlns:req="{req_ns}" xmlns:i18n="{i18n_ns}" xmlns:oc="{op_ns}">'
            "<soapenv:Header>"
            "<wsse:Security>"
            "<wsse:UsernameToken>"
            f"<wsse:Username>{username}</wsse:Username>"
            f'<wsse:Password Type="{pw_type}">{password}</wsse:Password>'
            "</wsse:UsernameToken>"
            "</wsse:Security>"
            "<i18n:international>"
            "<i18n:locale>en</i18n:locale>"
            "</i18n:international>"
            "</soapenv:Header>"
            "<soapenv:Body>"
            "<oc:performOCWithSummary>"
            "<arg0>"
            "<req:ID>"
            f"<req:MessageID>{xml_escape(message_id)}</req:MessageID>"
            "</req:ID>"
            "<req:Product>"
            "<req:SubjectProperty>"
            f"<req:TitleNumber>{title}</req:TitleNumber>"
            "</req:SubjectProperty>"
            "<req:ExternalReference>"
            f"<req:Reference>{ref}</req:Reference>"
            "<req:AllocatedBy>PropertyEye</req:AllocatedBy>"
            f"<req:Description>{description}</req:Description>"
            "</req:ExternalReference>"
            "<req:CustomerReference>"
            f"<req:Reference>{ref}</req:Reference>"
            "<req:AllocatedBy>PropertyEye</req:AllocatedBy>"
            f"<req:Description>{description}</req:Description>"
            "</req:CustomerReference>"
            "<req:TitleKnownOfficialCopy>"
            "<req:ContinueIfTitleIsClosedAndContinuedIndicator>true</req:ContinueIfTitleIsClosedAndContinuedIndicator>"
            "<req:NotifyIfPendingFirstRegistrationIndicator>false</req:NotifyIfPendingFirstRegistrationIndicator>"
            "<req:NotifyIfPendingApplicationIndicator>false</req:NotifyIfPendingApplicationIndicator>"
            "<req:SendBackDatedIndicator>false</req:SendBackDatedIndicator>"
            "<req:ContinueIfActualFeeExceedsExpectedFeeIndicator>true</req:ContinueIfActualFeeExceedsExpectedFeeIndicator>"
            "<req:IncludeTitlePlanIndicator>false</req:IncludeTitlePlanIndicator>"
            "</req:TitleKnownOfficialCopy>"
            "</req:Product>"
            "</arg0>"
            "</oc:performOCWithSummary>"
            "</soapenv:Body>"
            "</soapenv:Envelope>"
        )

    def _parse_live_response(
        self,
        report_id: str,
        title_number: str,
        raw_xml: str,
        vendor_name: Optional[str],
        property_address: str,
    ) -> dict[str, Any]:
        parsed = xmltodict.parse(raw_xml)
        gateway_response = self._find_first_key(parsed, {"GatewayResponse"}) or {}
        type_code = str(
            self._extract_text(self._find_first_key(gateway_response, {"TypeCode"})) or ""
        ).strip()
        logger.info(
            "Register Extract gateway response | report_id=%s | type_code=%s",
            report_id,
            type_code or None,
        )

        if not type_code:
            fault = self._find_first_key(parsed, {"Fault"}) or {}
            fault_code = self._extract_text(self._find_first_key(fault, {"faultcode", "Code"}))
            fault_string = self._extract_text(self._find_first_key(fault, {"faultstring", "Reason"}))
            if fault_code or fault_string:
                message = f"{fault_code or 'soap.fault'}: {fault_string or 'SOAP fault returned by Business Gateway.'}"
                logger.error(
                    "Register Extract SOAP fault | report_id=%s | code=%s | message=%s",
                    report_id,
                    fault_code or None,
                    fault_string or None,
                )
                payload = RegisterExtractResponseSchema(
                    report_id=report_id,
                    title_number=title_number,
                    fetched_at=datetime.utcnow(),
                    status="failed",
                    property=RegisterExtractPropertySchema(
                        address=property_address,
                        description=property_address,
                    ),
                    official_copy_available=False,
                    error_message=message,
                )
                return {"payload": payload, "raw_xml": raw_xml, "pdf_bytes": None}
            logger.warning(
                "Register Extract response missing GatewayResponse | report_id=%s",
                report_id,
            )
            payload = RegisterExtractResponseSchema(
                report_id=report_id,
                title_number=title_number,
                fetched_at=datetime.utcnow(),
                status="failed",
                property=RegisterExtractPropertySchema(
                    address=property_address,
                    description=property_address,
                ),
                official_copy_available=False,
                error_message="Register Extract response did not include a gateway response.",
            )
            return {"payload": payload, "raw_xml": raw_xml, "pdf_bytes": None}

        if type_code == "10":
            acknowledgement = self._find_first_key(gateway_response, {"Acknowledgement"}) or {}
            details = self._find_first_key(acknowledgement, {"AcknowledgementDetails"}) or acknowledgement
            message = self._extract_text(details.get("MessageDescription")) or "Request queued by Business Gateway."
            logger.info(
                "Register Extract response type=acknowledgement | report_id=%s | message=%s",
                report_id,
                message,
            )
            payload = RegisterExtractResponseSchema(
                report_id=report_id,
                title_number=title_number,
                fetched_at=datetime.utcnow(),
                status="pending",
                property=RegisterExtractPropertySchema(),
                official_copy_available=False,
                error_message=message,
            )
            return {"payload": payload, "attachment_bytes": None}

        if type_code == "20":
            rejection = self._find_first_key(gateway_response, {"Rejection"}) or {}
            rejection_response = self._find_first_key(rejection, {"RejectionResponse"}) or rejection
            reason = (
                self._extract_text(self._find_first_key(rejection_response, {"Reason"}))
                or "Register extract request rejected."
            )
            code = self._extract_text(self._find_first_key(rejection_response, {"Code"}))
            logger.warning(
                "Register Extract response type=rejection | report_id=%s | code=%s | reason=%s",
                report_id,
                code or None,
                reason,
            )
            payload = RegisterExtractResponseSchema(
                report_id=report_id,
                title_number=title_number,
                fetched_at=datetime.utcnow(),
                status="failed",
                property=RegisterExtractPropertySchema(),
                official_copy_available=False,
                error_message=f"{code}: {reason}" if code else reason,
            )
            return {"payload": payload, "attachment_bytes": None}

        if type_code not in {"30"}:
            logger.error(
                "Register Extract response returned unrecognised gateway type | report_id=%s | type_code=%s",
                report_id,
                type_code or None,
            )
            payload = RegisterExtractResponseSchema(
                report_id=report_id,
                title_number=title_number,
                fetched_at=datetime.utcnow(),
                status="failed",
                property=RegisterExtractPropertySchema(
                    address=property_address,
                    description=property_address,
                ),
                official_copy_available=False,
                error_message=(
                    self._extract_text(
                        self._find_first_key(gateway_response, {"MessageDescription", "Reason"})
                    )
                    or "Register Extract response did not include a recognised gateway status."
                ),
            )
            return {"payload": payload, "attachment_bytes": None}

        results = self._find_first_key(gateway_response, {"Results"}) or {}
        summary = self._find_first_key(results, {"OCSummaryData"}) or {}
        register_data = self._find_first_key(results, {"OCRegisterData"}) or {}
        attachment = self._find_first_key(results, {"Attachment"})
        attachment_bytes = self._extract_attachment_bytes(attachment)

        property_address = self._extract_text(
            self._find_first_key(summary, {"PropertyAddress", "PropertyDescription", "Description"})
        )
        tenure = self._extract_text(self._find_first_key(summary, {"Tenure", "TenureDescription"}))
        description = self._extract_text(
            self._find_first_key(register_data, {"PropertyDescription", "Description"})
        ) or property_address

        proprietors = [
            self._build_proprietor(node, vendor_name)
            for node in self._find_nodes_by_fragment(summary, "proprietor")
        ]
        if not proprietors:
            proprietors = [
                self._build_proprietor(node, vendor_name)
                for node in self._find_nodes_by_fragment(register_data, "proprietor")
            ]

        charges = self._build_entries(self._find_nodes_by_fragment(summary, "charge"))
        restrictions = self._build_entries(self._find_nodes_by_fragment(summary, "restriction"))
        leases = self._build_entries(self._find_nodes_by_fragment(summary, "lease"))
        notices = self._build_entries(
            self._find_nodes_by_fragment(summary, "notice")
            + self._find_nodes_by_fragment(summary, "caution")
        )
        quick_reference_flags: list[str] = []
        if vendor_name and proprietors and any(item.mismatch for item in proprietors):
            quick_reference_flags.append("Owner name differs from agency seller name")
        if restrictions:
            quick_reference_flags.append("Restrictions present on title")
        if charges:
            quick_reference_flags.append("Charges or mortgages present")
        if leases:
            quick_reference_flags.append("Lease entries returned")

        payload = RegisterExtractResponseSchema(
            report_id=report_id,
            title_number=title_number,
            fetched_at=datetime.utcnow(),
            status="complete",
            property=RegisterExtractPropertySchema(
                address=property_address,
                tenure=tenure,
                description=description,
            ),
            proprietors=proprietors,
            charges=charges,
            restrictions=restrictions,
            leases=leases,
            notices=notices,
            quick_reference_flags=quick_reference_flags,
            official_copy_available=bool(attachment_bytes),
        )
        return {"payload": payload, "attachment_bytes": attachment_bytes}

    def _build_proprietor(
        self,
        node: dict[str, Any],
        vendor_name: Optional[str],
    ) -> RegisterExtractProprietorSchema:
        name = self._extract_text(self._find_first_key(node, {"Name", "ProprietorName"}))
        proprietor_type = self._extract_text(self._find_first_key(node, {"Type", "ProprietorType"})) or "Unknown"
        address = self._join_address_parts(
            self._find_first_key(node, {"Address", "AddressDetails", "ProprietorAddress"}) or node
        )
        mismatch = bool(vendor_name and name and not self._names_match(vendor_name, name))
        return RegisterExtractProprietorSchema(
            name=name,
            type=proprietor_type,
            address=address,
            mismatch=mismatch,
        )

    def _build_entries(self, nodes: list[dict[str, Any]]) -> list[RegisterExtractEntrySchema]:
        entries: list[RegisterExtractEntrySchema] = []
        for node in nodes:
            entry_text = self._extract_text(self._find_first_key(node, {"EntryText", "Text", "Description"}))
            if not entry_text:
                continue
            entries.append(
                RegisterExtractEntrySchema(
                    entry_number=self._extract_text(self._find_first_key(node, {"EntryNumber"})),
                    entry_text=entry_text,
                    registration_date=self._extract_text(
                        self._find_first_key(node, {"RegistrationDate", "Date"})
                    ),
                    raw=node,
                )
            )
        return entries

    def _find_first_key(self, value: Any, key_names: set[str]) -> Any:
        if isinstance(value, dict):
            for key, child in value.items():
                local_key = key.split(":")[-1]
                if key in key_names or local_key in key_names:
                    return child
                found = self._find_first_key(child, key_names)
                if found is not None:
                    return found
        elif isinstance(value, list):
            for item in value:
                found = self._find_first_key(item, key_names)
                if found is not None:
                    return found
        return None

    def _find_nodes_by_fragment(self, value: Any, fragment: str) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        fragment = fragment.lower()
        if isinstance(value, dict):
            for key, child in value.items():
                if fragment in key.lower():
                    if isinstance(child, list):
                        matches.extend([item for item in child if isinstance(item, dict)])
                    elif isinstance(child, dict):
                        matches.append(child)
                matches.extend(self._find_nodes_by_fragment(child, fragment))
        elif isinstance(value, list):
            for item in value:
                matches.extend(self._find_nodes_by_fragment(item, fragment))
        return matches

    def _extract_attachment_bytes(self, attachment: Any) -> Optional[bytes]:
        attachment_text = self._extract_text(
            self._find_first_key(
                attachment,
                {
                    "BinaryData",
                    "DocumentBinary",
                    "AttachmentBinaryObject",
                    "EmbeddedDocumentBinaryObject",
                    "Value",
                },
            )
        )
        if not attachment_text:
            return None
        try:
            return base64.b64decode(attachment_text)
        except Exception:
            return None

    def _extract_pdf_bytes(self, attachment_bytes: Optional[bytes]) -> Optional[bytes]:
        if not attachment_bytes:
            return None
        if attachment_bytes.startswith(b"%PDF"):
            return attachment_bytes
        if attachment_bytes.startswith(b"PK"):
            try:
                with zipfile.ZipFile(io.BytesIO(attachment_bytes)) as zf:
                    for name in zf.namelist():
                        if name.lower().endswith(".pdf"):
                            return zf.read(name)
            except Exception:
                return None
        return None

    def _join_address_parts(self, value: Any) -> Optional[str]:
        if isinstance(value, str):
            return value.strip() or None
        if isinstance(value, dict):
            parts: list[str] = []
            for key, child in value.items():
                lowered = key.lower()
                if "address" in lowered or "line" in lowered or "postcode" in lowered:
                    text = self._extract_text(child)
                    if text:
                        parts.append(text)
            if parts:
                return ", ".join(dict.fromkeys(parts))
        return None

    def _extract_text(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            cleaned = re.sub(r"\s+", " ", value).strip()
            return cleaned or None
        if isinstance(value, dict):
            for key in ("#text", "Text", "Description", "Value", "Reference"):
                if key in value:
                    return self._extract_text(value[key])
            parts = [self._extract_text(item) for item in value.values()]
            joined = " ".join([part for part in parts if part])
            return joined or None
        if isinstance(value, list):
            parts = [self._extract_text(item) for item in value]
            joined = " ".join([part for part in parts if part])
            return joined or None
        return str(value)

    def _names_match(self, left: str, right: str) -> bool:
        normalize = lambda value: re.sub(r"[^A-Z0-9]", "", value.upper())
        return normalize(left) == normalize(right)

    def _make_message_id(self, report_id: str) -> str:
        compact = re.sub(r"[^A-Za-z0-9\-]", "", report_id)[:40]
        if len(compact) < 5:
            compact = f"pe-{uuid.uuid4().hex[:8]}"
        return compact

    def _build_mock_extract(self, fraud_match: FraudMatch) -> RegisterExtractResponseSchema:
        owner_name = "JOHN REGISTRY OWNER"
        vendor_name = fraud_match.property_listing.vendor_name or ""
        return RegisterExtractResponseSchema(
            report_id=fraud_match.id,
            title_number=fraud_match.property_listing.title_number or MOCK_TITLE_NUMBER,
            fetched_at=datetime.utcnow(),
            status="complete",
            property=RegisterExtractPropertySchema(
                address=fraud_match.property_listing.address,
                tenure="Freehold",
                description="Official copy summary data returned from HM Land Registry vendor test data.",
            ),
            proprietors=[
                RegisterExtractProprietorSchema(
                    name=owner_name,
                    type="Individual",
                    address="1 REGISTRY STREET, BROXBOURNE, EN10 7HJ",
                    mismatch=bool(vendor_name and not self._names_match(vendor_name, owner_name)),
                )
            ],
            charges=[
                RegisterExtractEntrySchema(
                    entry_number="C1",
                    entry_text="A charge dated 01/02/2024 in favour of Example Bank plc.",
                    registration_date="2024-02-01",
                )
            ],
            restrictions=[
                RegisterExtractEntrySchema(
                    entry_number="B2",
                    entry_text="No disposition by a sole proprietor except under an order of the court.",
                )
            ],
            leases=[],
            notices=[],
            quick_reference_flags=["Owner name differs from agency seller name"],
            official_copy_available=True,
        )
