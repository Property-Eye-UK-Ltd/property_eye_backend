"""Register Extract Service integration and caching."""

from __future__ import annotations

import base64
import io
import logging
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
        if cached and cached.status == "complete" and cached.parsed_json:
            return RegisterExtractResponseSchema.model_validate(cached.parsed_json)

        if mock:
            payload = self._build_mock_extract(fraud_match)
            await self._save_extract(
                fraud_match=fraud_match,
                payload=payload,
                raw_xml="<MockRegisterExtract />",
                db=db,
            )
            return payload

        title_number = (fraud_match.property_listing.title_number or "").strip().upper()
        if not title_number:
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

    async def get_pdf_bytes(
        self,
        report_id: str,
        db: AsyncSession,
        mock: bool = False,
    ) -> tuple[bytes, str]:
        fraud_match = await self._get_fraud_match(report_id, db)
        if mock:
            return MOCK_PDF_BYTES, f"register-extract-{MOCK_TITLE_NUMBER}.pdf"

        title_number = (fraud_match.property_listing.title_number or "").strip().upper()
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
            if not settings.HMLR_RES_PATH:
                raise ValueError("HMLR_RES_PATH is not configured.")

            response = await client.client.post(
                settings.HMLR_RES_PATH,
                content=self._build_live_request_xml(
                    message_id=self._make_message_id(report_id),
                    title_number=title_number,
                    property_address=property_address,
                ).encode("utf-8"),
                headers={"Content-Type": "text/xml; charset=utf-8"},
            )
            parsed = self._parse_live_response(
                report_id=report_id,
                title_number=title_number,
                raw_xml=response.text,
                vendor_name=vendor_name,
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
        return (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<RequestOCWithSummaryV2_0 '
            'xmlns="http://www.oscre.org/ns/eReg-Final/2011/RequestOCWithSummaryV2_0">'
            "<LoginDetails>"
            f"<UserID>{username}</UserID>"
            f"<Password>{password}</Password>"
            "<UserLocale>en</UserLocale>"
            "</LoginDetails>"
            "<ID>"
            f"<MessageID>{xml_escape(message_id)}</MessageID>"
            "</ID>"
            "<Product>"
            "<ExternalReference>"
            f"<Reference>{ref}</Reference>"
            "<AllocatedBy>PropertyEye</AllocatedBy>"
            f"<Description>{description}</Description>"
            "</ExternalReference>"
            "<CustomerReference>"
            f"<Reference>{ref}</Reference>"
            "<AllocatedBy>PropertyEye</AllocatedBy>"
            f"<Description>{description}</Description>"
            "</CustomerReference>"
            "<TitleKnownOfficialCopy>"
            "<ContinueIfTitleIsClosedAndContinuedIndicator>false</ContinueIfTitleIsClosedAndContinuedIndicator>"
            "<NotifyIfPendingFirstRegistrationIndicator>false</NotifyIfPendingFirstRegistrationIndicator>"
            "<NotifyIfPendingApplicationIndicator>false</NotifyIfPendingApplicationIndicator>"
            "<SendBackDatedIndicator>false</SendBackDatedIndicator>"
            "<ContinueIfActualFeeExceedsExpectedFeeIndicator>false</ContinueIfActualFeeExceedsExpectedFeeIndicator>"
            "<IncludeTitlePlanIndicator>true</IncludeTitlePlanIndicator>"
            f"<TitleNumber>{title}</TitleNumber>"
            f"<PropertyDescription>{description}</PropertyDescription>"
            "</TitleKnownOfficialCopy>"
            "</Product>"
            "</RequestOCWithSummaryV2_0>"
        )

    def _parse_live_response(
        self,
        report_id: str,
        title_number: str,
        raw_xml: str,
        vendor_name: Optional[str],
    ) -> dict[str, Any]:
        parsed = xmltodict.parse(raw_xml)
        gateway_response = self._find_first_key(parsed, {"GatewayResponse"}) or {}
        type_code = str(self._extract_text(gateway_response.get("TypeCode")) or "").strip()

        if type_code == "10":
            acknowledgement = self._find_first_key(gateway_response, {"Acknowledgement"}) or {}
            details = self._find_first_key(acknowledgement, {"AcknowledgementDetails"}) or acknowledgement
            payload = RegisterExtractResponseSchema(
                report_id=report_id,
                title_number=title_number,
                fetched_at=datetime.utcnow(),
                status="pending",
                property=RegisterExtractPropertySchema(),
                official_copy_available=False,
                error_message=self._extract_text(details.get("MessageDescription")) or "Request queued by Business Gateway.",
            )
            return {"payload": payload, "attachment_bytes": None}

        if type_code == "20":
            rejection = self._find_first_key(gateway_response, {"Rejection"}) or {}
            payload = RegisterExtractResponseSchema(
                report_id=report_id,
                title_number=title_number,
                fetched_at=datetime.utcnow(),
                status="failed",
                property=RegisterExtractPropertySchema(),
                official_copy_available=False,
                error_message=self._extract_text(rejection.get("Reason")) or "Register extract request rejected.",
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
                if key in key_names:
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
