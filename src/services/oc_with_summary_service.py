"""HM Land Registry Official Copy with Summary service integration."""

from __future__ import annotations

import base64
import logging
import re
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional
from xml.sax.saxutils import escape as xml_escape

import xmltodict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from src.core.config import settings
from src.models.fraud_match import FraudMatch
from src.models.oc_with_summary import OCWithSummary
from src.models.property_listing import PropertyListing
from src.schemas.oc_with_summary import (
    OCAttachmentSchema,
    OCChargeSchema,
    OCEntryDetailsSchema,
    OCIndividualNameSchema,
    OCOrganisationNameSchema,
    OCLicenseSchema,
    OCPartySchema,
    OCPricePaidSchema,
    OCProprietorNameSchema,
    OCProprietorshipDetailSchema,
    OCPollDetailsSchema,
    OCRestrictionSchema,
    OCWithSummaryResponseSchema,
    OCTitleDetailsSchema,
    OCAddressSchema,
)
from src.services.land_registry_client import LandRegistryClient

logger = logging.getLogger(__name__)

_TITLE_ABSOLUTE_MAP = {
    "10": "Absolute Freehold",
    "20": "Possessory Freehold",
    "30": "Qualified Freehold",
    "40": "Scheme Title - Freehold",
    "50": "Scheme Title - Leasehold",
    "60": "Absolute Leasehold",
    "70": "Good Leasehold",
    "80": "Qualified Leasehold",
    "90": "Possessory Leasehold",
    "100": "Absolute Rentcharge",
    "110": "Possessory Rentcharge",
    "120": "Qualified Rentcharge",
    "130": "Caution Against First Registration",
}
_MAX_INLINE_PDF_BYTES = 10 * 1024 * 1024


class OCWithSummaryService:
    """Fetch, cache, and poll OC with Summary responses."""

    def __init__(self) -> None:
        self._client: LandRegistryClient | None = None
        self._pdf_root = Path("data") / "oc_with_summary"

    def _get_client(self) -> LandRegistryClient:
        """Create the Business Gateway client lazily when network access is needed."""
        if self._client is None:
            self._client = LandRegistryClient()
        return self._client

    def _get_service_path(self) -> str:
        """Resolve the OC with Summary endpoint from config or BG environment."""
        return settings.HMLR_RES_PATH.strip() or self._get_client().oc_with_summary_path

    async def get_or_fetch(
        self,
        report_id: str,
        db: AsyncSession,
    ) -> OCWithSummaryResponseSchema:
        """Return cached OC with Summary data or fetch a new copy."""
        fraud_match = await self._get_fraud_match(report_id, db)
        cached = fraud_match.oc_with_summary
        if cached and cached.status == "complete" and cached.parsed_json:
            return OCWithSummaryResponseSchema.model_validate(cached.parsed_json)
        return await self._request_and_store(fraud_match, db)

    async def poll(
        self,
        poll_id: str,
        db: AsyncSession,
    ) -> OCWithSummaryResponseSchema:
        """Poll a queued OC with Summary request using the stored poll ID."""
        record = await self._get_record_by_poll_id(poll_id, db)
        if record.status == "complete" and record.parsed_json:
            return OCWithSummaryResponseSchema.model_validate(record.parsed_json)
        if not record.fraud_match:
            raise ValueError(f"OC with Summary record {poll_id} has no fraud report link")
        return await self._request_and_store(record.fraud_match, db, existing=record)

    async def _request_and_store(
        self,
        fraud_match: FraudMatch,
        db: AsyncSession,
        existing: Optional[OCWithSummary] = None,
    ) -> OCWithSummaryResponseSchema:
        """Submit the SOAP request and persist the resulting cache row."""
        title_number = await self._resolve_title_number(fraud_match, db)
        if not title_number:
            raise ValueError(
                "Title number is required to request an Official Copy with Summary."
            )

        record = existing or fraud_match.oc_with_summary or OCWithSummary(
            fraud_report_id=fraud_match.id
        )
        if fraud_match.oc_with_summary is None and existing is None:
            db.add(record)

        raw_xml: Optional[str] = None
        try:
            soap_xml = self._build_request_xml(
                report_id=fraud_match.id,
                title_number=title_number,
                customer_reference=fraud_match.property_listing.agency.name
                if fraud_match.property_listing.agency
                else fraud_match.property_listing.id,
                description=fraud_match.property_listing.address,
            )
            logger.info(
                "OC with Summary request | report_id=%s | title_number=%s | service_path=%s",
                fraud_match.id,
                title_number,
                self._get_service_path(),
            )
            client = self._get_client()
            response = await client.client.post(
                self._get_service_path(),
                content=soap_xml.encode("utf-8"),
                headers={"Content-Type": "text/xml; charset=utf-8"},
            )
            raw_xml = response.text
            parsed = self._parse_response(
                report_id=fraud_match.id,
                raw_xml=raw_xml,
            )
            await self._store_record(record, parsed, raw_xml, db)
            return parsed
        except Exception as exc:
            await db.rollback()
            logger.error(
                "OC with Summary request failed for report %s: %s",
                fraud_match.id,
                exc,
            )
            try:
                await self._mark_failed(record, raw_xml, str(exc), db)
            except Exception as mark_exc:
                logger.error(
                    "Failed to mark OC with Summary request %s as failed: %s",
                    fraud_match.id,
                    mark_exc,
                )
            raise
        finally:
            if self._client is not None:
                await self._client.close()
                self._client = None

    async def _resolve_title_number(
        self,
        fraud_match: FraudMatch,
        db: AsyncSession,
    ) -> str:
        """Resolve a title number from the listing, prior extracts, or address search."""
        listing_title = (fraud_match.property_listing.title_number or "").strip().upper()
        if listing_title:
            logger.info(
                "OC with Summary title source | report_id=%s | source=listing | title_number=%s",
                fraud_match.id,
                listing_title,
            )
            return listing_title

        if fraud_match.oc_with_summary and fraud_match.oc_with_summary.title_number:
            cached_title = fraud_match.oc_with_summary.title_number.strip().upper()
            if cached_title:
                logger.info(
                    "OC with Summary title source | report_id=%s | source=cache | title_number=%s",
                    fraud_match.id,
                    cached_title,
                )
                return cached_title

        if fraud_match.register_extract and fraud_match.register_extract.title_number:
            cached_extract_title = fraud_match.register_extract.title_number.strip().upper()
            if cached_extract_title:
                fraud_match.property_listing.title_number = cached_extract_title
                await db.commit()
                logger.info(
                    "OC with Summary title source | report_id=%s | source=register_extract | title_number=%s",
                    fraud_match.id,
                    cached_extract_title,
                )
                return cached_extract_title

        address = fraud_match.property_listing.address or fraud_match.ppd_full_address or ""
        postcode = fraud_match.property_listing.postcode or fraud_match.ppd_postcode or ""
        if not address or not postcode:
            logger.info(
                "OC with Summary title source | report_id=%s | source=none | address_present=%s | postcode_present=%s",
                fraud_match.id,
                bool(address),
                bool(postcode),
            )
            return ""

        logger.info(
            "OC with Summary property-description search start | report_id=%s | address=%s | postcode=%s",
            fraud_match.id,
            address,
            postcode,
        )
        client = self._get_client()
        result = await client.search_title_by_property_description(
            property_address=address,
            postcode=postcode,
            message_id=fraud_match.id,
            customer_reference=fraud_match.property_listing.agency.name
            if fraud_match.property_listing.agency
            else None,
        )

        title_number = getattr(result, "title_number", None) or ""
        if title_number:
            fraud_match.property_listing.title_number = title_number
            await db.commit()
            logger.info(
                "OC with Summary title source | report_id=%s | source=property_description | title_number=%s",
                fraud_match.id,
                title_number,
            )
        else:
            logger.info(
                "OC with Summary title source | report_id=%s | source=none | search_status=%s",
                fraud_match.id,
                getattr(result, "status_code", None),
            )
        return title_number

    async def _store_record(
        self,
        record: OCWithSummary,
        payload: OCWithSummaryResponseSchema,
        raw_xml: Optional[str],
        db: AsyncSession,
    ) -> None:
        """Persist the parsed payload and attachment metadata."""
        record.title_number = payload.title_number
        record.response_code = payload.response_code
        record.status = payload.status
        record.poll_id = payload.poll_details.poll_id if payload.poll_details else None
        record.expected_at = (
            payload.poll_details.expected_at if payload.poll_details else None
        )
        record.raw_xml = raw_xml
        record.parsed_json = payload.model_dump(mode="json")
        record.fetched_at = payload.fetched_at
        if payload.attachments:
            attachment = payload.attachments[0]
            record.pdf_filename = attachment.filename
        if payload.official_copy_available and raw_xml:
            attachment_bytes = self._extract_pdf_bytes(raw_xml)
            if attachment_bytes:
                if len(attachment_bytes) <= _MAX_INLINE_PDF_BYTES:
                    record.pdf_base64 = base64.b64encode(attachment_bytes).decode("ascii")
                else:
                    pdf_path = self._write_pdf_to_disk(
                        attachment_bytes,
                        record.pdf_filename or f"official_copy_{payload.title_number or 'document'}.pdf",
                    )
                    record.pdf_filename = pdf_path.name
                    record.pdf_base64 = None
        await db.commit()

    async def _mark_failed(
        self,
        record: OCWithSummary,
        raw_xml: Optional[str],
        error_message: str,
        db: AsyncSession,
    ) -> None:
        """Persist a failed status even when request parsing or save fails."""
        record.response_code = record.response_code or "Rejection"
        record.status = "failed"
        record.raw_xml = raw_xml
        record.parsed_json = {
            "status": "failed",
            "response_code": record.response_code,
            "error_message": error_message,
        }
        record.fetched_at = datetime.utcnow()
        await db.commit()

    async def _get_fraud_match(self, report_id: str, db: AsyncSession) -> FraudMatch:
        stmt = (
            select(FraudMatch)
            .options(
                joinedload(FraudMatch.property_listing).joinedload(PropertyListing.agency),
                joinedload(FraudMatch.oc_with_summary),
                joinedload(FraudMatch.register_extract),
            )
            .where(FraudMatch.id == report_id)
        )
        result = await db.execute(stmt)
        fraud_match = result.scalar_one_or_none()
        if not fraud_match:
            raise ValueError(f"Fraud report {report_id} not found")
        return fraud_match

    async def _get_record_by_poll_id(self, poll_id: str, db: AsyncSession) -> OCWithSummary:
        stmt = (
            select(OCWithSummary)
            .options(
                joinedload(OCWithSummary.fraud_match)
                .joinedload(FraudMatch.property_listing)
                .joinedload(PropertyListing.agency)
            )
            .where(OCWithSummary.poll_id == poll_id)
        )
        result = await db.execute(stmt)
        record = result.scalar_one_or_none()
        if not record:
            raise ValueError(f"Poll ID {poll_id} not found")
        return record

    def _build_request_xml(
        self,
        report_id: str,
        title_number: str,
        customer_reference: str,
        description: Optional[str],
    ) -> str:
        """Build the SOAP request envelope for RequestOCWithSummaryV2_0."""
        message_id = str(uuid.uuid4())
        description_text = xml_escape((description or title_number)[:500])
        customer_reference_text = xml_escape((customer_reference or report_id)[:25])
        username = xml_escape(settings.HMLR_BG_USERNAME)
        password = xml_escape(settings.HMLR_BG_PASSWORD)
        i18n_ns = "http://www.w3.org/2005/09/ws-i18n"
        wsse_ns = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
        req_ns = "http://www.oscre.org/ns/eReg-Final/2011/RequestOCWithSummaryV2_0"
        pw_type = (
            "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText"
        )
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
            f'xmlns:wsse="{wsse_ns}" xmlns:req="{req_ns}" xmlns:i18n="{i18n_ns}">'
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
            '<req:RequestOCWithSummaryV2_0>'
            "<req:ID>"
            f"<req:MessageID>{xml_escape(message_id)}</req:MessageID>"
            "</req:ID>"
            "<req:Product>"
            "<req:SubjectProperty>"
            f"<req:TitleNumber>{xml_escape(title_number)}</req:TitleNumber>"
            "</req:SubjectProperty>"
            "<req:ExternalReference>"
            f"<req:Reference>{xml_escape(report_id[:25])}</req:Reference>"
            "<req:AllocatedBy>PropertyEye</req:AllocatedBy>"
            f"<req:Description>{description_text}</req:Description>"
            "</req:ExternalReference>"
            "<req:CustomerReference>"
            f"<req:Reference>{customer_reference_text}</req:Reference>"
            "<req:AllocatedBy>PropertyEye</req:AllocatedBy>"
            f"<req:Description>{description_text}</req:Description>"
            "</req:CustomerReference>"
            "<req:TitleKnownOfficialCopy>"
            "<req:ContinueIfTitleIsClosedAndContinuedIndicator>true</req:ContinueIfTitleIsClosedAndContinuedIndicator>"
            "<req:NotifyIfPendingFirstRegistrationIndicator>false</req:NotifyIfPendingFirstRegistrationIndicator>"
            "<req:NotifyIfPendingApplicationIndicator>false</req:NotifyIfPendingApplicationIndicator>"
            "<req:SendBackDatedIndicator>false</req:SendBackDatedIndicator>"
            "<req:ContinueIfActualFeeExceedsExpectedFeeIndicator>false</req:ContinueIfActualFeeExceedsExpectedFeeIndicator>"
            "<req:IncludeTitlePlanIndicator>false</req:IncludeTitlePlanIndicator>"
            "</req:TitleKnownOfficialCopy>"
            "</req:Product>"
            "</req:RequestOCWithSummaryV2_0>"
            "</soapenv:Body>"
            "</soapenv:Envelope>"
        )
        return body

    def _parse_response(
        self,
        report_id: str,
        raw_xml: str,
    ) -> OCWithSummaryResponseSchema:
        """Parse OC with Summary SOAP XML into the application schema."""
        parsed = xmltodict.parse(raw_xml)
        gateway_response = self._find_first_key(parsed, {"GatewayResponse"}) or {}
        type_code = str(self._extract_text(gateway_response.get("TypeCode")) or "").strip()

        if type_code == "10":
            ack = self._find_first_key(gateway_response, {"Acknowledgement"}) or {}
            details = self._find_first_key(ack, {"AcknowledgementDetails"}) or ack
            poll = OCPollDetailsSchema(
                poll_id=self._extract_text(self._find_first_key(details, {"UniqueID", "MessageID"})),
                expected_at=self._parse_datetime(
                    self._extract_text(self._find_first_key(details, {"ExpectedResponseDateTime"}))
                ),
                message_text=self._extract_text(self._find_first_key(details, {"MessageDescription"})),
            )
            return OCWithSummaryResponseSchema(
                report_id=report_id,
                fetched_at=datetime.utcnow(),
                status="pending",
                response_code="Acknowledgement",
                poll_details=poll,
                official_copy_available=False,
                error_message=poll.message_text,
            )

        if type_code == "20":
            rejection = self._find_first_key(gateway_response, {"Rejection"}) or {}
            response = self._find_first_key(rejection, {"RejectionResponse"}) or rejection
            code = self._extract_text(self._find_first_key(response, {"Code"}))
            reason = self._extract_text(self._find_first_key(response, {"Reason"}))
            raise ValueError(f"{code or 'Rejection'}: {reason or 'Request rejected by Business Gateway.'}")

        results = self._find_first_key(gateway_response, {"Results"}) or {}
        summary = self._find_first_key(results, {"OCSummaryData"}) or {}
        register_data = self._find_first_key(results, {"OCRegisterData"}) or {}
        attachment = self._find_first_key(results, {"Attachment"})

        price_paid = self._build_price_paid(summary)
        title = self._build_title_details(summary)
        proprietorship = self._build_proprietorship(summary)
        charges = self._build_charges(summary, register_data)
        restrictions = self._build_restrictions(summary)
        leases = self._build_leases(summary)
        attachments = self._build_attachments(attachment)

        title_number = title.title_number if title else None
        return OCWithSummaryResponseSchema(
            report_id=report_id,
            title_number=title_number,
            fetched_at=datetime.utcnow(),
            status="complete",
            response_code="Result",
            title_details=title,
            proprietorship_details=proprietorship,
            charges=charges,
            restrictions=restrictions,
            leases=leases,
            price_paid=price_paid,
            attachments=attachments,
            poll_details=None,
            official_copy_available=bool(attachments),
        )

    def _build_title_details(self, summary: Any) -> Optional[OCTitleDetailsSchema]:
        title = self._find_first_key(summary, {"Title"}) or {}
        registration = self._find_first_key(title, {"TitleRegistrationDetails"}) or {}
        class_code = self._extract_text(self._find_first_key(title, {"ClassOfTitleCode"}))
        property_address = self._build_address_schema(
            self._find_first_key(summary, {"PropertyAddress"})
        )
        return OCTitleDetailsSchema(
            title_number=self._extract_text(self._find_first_key(title, {"TitleNumber"})),
            title_absolute=_TITLE_ABSOLUTE_MAP.get(class_code or "", class_code),
            property_address=property_address,
            district_name=self._extract_text(self._find_first_key(registration, {"DistrictName"})),
            administrative_area=self._extract_text(self._find_first_key(registration, {"AdministrativeArea"})),
            land_registry_office_name=self._extract_text(
                self._find_first_key(registration, {"LandRegistryOfficeName"})
            ),
            latest_edition_date=self._parse_date(
                self._extract_text(self._find_first_key(registration, {"LatestEditionDate"}))
            ),
        )

    def _build_proprietorship(self, summary: Any) -> list[OCProprietorshipDetailSchema]:
        node = self._find_first_key(summary, {"Proprietorship"}) or {}
        if not node:
            return []
        parties = self._find_nodes_by_fragment(node, "RegisteredProprietorParty")
        if not parties:
            parties = self._find_nodes_by_fragment(node, "CautionerParty")
        return [
            OCProprietorshipDetailSchema(
                current_proprietorship_date=self._parse_date(
                    self._extract_text(self._find_first_key(node, {"CurrentProprietorshipDate"}))
                ),
                registered_proprietor_party=[
                    self._build_party(party)
                    for party in self._find_nodes_by_fragment(node, "RegisteredProprietorParty")
                ],
                cautioner_party=[
                    self._build_party(party)
                    for party in self._find_nodes_by_fragment(node, "CautionerParty")
                ],
            )
        ]

    def _build_party(self, node: dict[str, Any]) -> OCPartySchema:
        individual = self._find_first_key(node, {"PrivateIndividual"}) or {}
        organisation = self._find_first_key(node, {"Organization"}) or {}
        proprietor_name: Optional[OCProprietorNameSchema] = None
        if individual:
            name = self._find_first_key(individual, {"Name"}) or {}
            proprietor_name = OCProprietorNameSchema(
                individual_name=OCIndividualNameSchema(
                    forename=self._extract_text(self._find_first_key(name, {"ForenamesName"})),
                    surname=self._extract_text(self._find_first_key(name, {"SurnameName"})),
                )
            )
        elif organisation:
            proprietor_name = OCProprietorNameSchema(
                organisation_name=OCOrganisationNameSchema(
                    name=self._extract_text(self._find_first_key(organisation, {"Name"}))
                )
            )

        addresses = self._find_nodes_by_fragment(node, "Address")
        address_schema = self._build_address_schema(addresses[0]) if addresses else None
        return OCPartySchema(
            proprietor_name=proprietor_name,
            company_registration_number=self._extract_text(
                self._find_first_key(organisation, {"CompanyRegistrationNumber"})
            ),
            proprietor_address=address_schema,
            proprietorship_date=self._parse_date(
                self._extract_text(self._find_first_key(node, {"CurrentProprietorshipDate"}))
            ),
            trading_name=self._extract_text(self._find_first_key(node, {"TradingName"})),
            party_number=self._extract_text(self._find_first_key(node, {"PartyNumber"})),
            party_description=self._extract_text(self._find_first_key(node, {"PartyDescription"})),
        )

    def _build_price_paid(self, summary: Any) -> Optional[OCPricePaidSchema]:
        price_paid = self._find_first_key(summary, {"PricePaidEntry"}) or {}
        if not price_paid:
            return None
        entry = self._find_first_key(price_paid, {"EntryDetails"}) or {}
        amount = self._extract_text(self._find_first_key(entry, {"Amount"}))
        return OCPricePaidSchema(
            amount=amount,
            date=self._parse_date(self._extract_text(self._find_first_key(entry, {"Date"}))),
        )

    def _build_charges(self, summary: Any, register_data: Any) -> list[OCChargeSchema]:
        charges = self._find_first_key(summary, {"Charge"}) or self._find_first_key(register_data, {"Charge"}) or {}
        entries = self._find_nodes_by_fragment(charges, "ChargeEntry")
        return [
            OCChargeSchema(
                charge_id=self._extract_text(self._find_first_key(entry, {"ChargeID"})),
                charge_date=self._parse_date(self._extract_text(self._find_first_key(entry, {"ChargeDate"}))),
                registered_charge=self._coerce_dict(self._find_first_key(entry, {"RegisteredCharge"})),
                charge_proprietor=self._coerce_dict(self._find_first_key(entry, {"ChargeProprietor"})),
                sub_charges=self._coerce_list(self._find_first_key(entry, {"SubCharge"})),
            )
            for entry in entries
        ]

    def _build_restrictions(self, summary: Any) -> list[OCRestrictionSchema]:
        restrictions = self._find_first_key(summary, {"RestrictionDetails"}) or {}
        entries = self._find_nodes_by_fragment(restrictions, "RestrictionEntry")
        built: list[OCRestrictionSchema] = []
        for entry in entries:
            payload = self._find_first_key(entry, {"ChargeRelatedRestriction", "ChargeRestriction", "NonChargeRestriction"}) or {}
            built.append(
                OCRestrictionSchema(
                    restriction_type_code=self._extract_text(
                        self._find_first_key(payload, {"RestrictionTypeCode"})
                    ),
                    entry_details=self._build_entry_details(payload),
                    raw=payload if isinstance(payload, dict) else {},
                )
            )
        return built

    def _build_leases(self, summary: Any) -> list[OCLicenseSchema]:
        leases = self._find_first_key(summary, {"Lease"}) or {}
        entries = self._find_nodes_by_fragment(leases, "LeaseEntry")
        built: list[OCLicenseSchema] = []
        for entry in entries:
            built.append(
                OCLicenseSchema(
                    lease_term=self._extract_text(self._find_first_key(entry, {"LeaseTerm"})),
                    lease_date=self._parse_date(self._extract_text(self._find_first_key(entry, {"LeaseDate"}))),
                    rent=self._extract_text(self._find_first_key(entry, {"Rent"})),
                    lease_party=[
                        self._build_party(party)
                        for party in self._find_nodes_by_fragment(entry, "LeaseParty")
                    ],
                    entry_details=self._build_entry_details(entry),
                    raw=entry if isinstance(entry, dict) else {},
                )
            )
        return built

    def _build_attachments(self, attachment: Any) -> list[OCAttachmentSchema]:
        if not attachment:
            return []
        attachments = attachment if isinstance(attachment, list) else [attachment]
        built: list[OCAttachmentSchema] = []
        for item in attachments:
            built.append(
                OCAttachmentSchema(
                    filename=(item or {}).get("@filename") if isinstance(item, dict) else None,
                    mime_type=(item or {}).get("@mimeCode") if isinstance(item, dict) else None,
                )
            )
        return built

    def _build_entry_details(self, node: Any) -> Optional[OCEntryDetailsSchema]:
        entry = self._find_first_key(node, {"EntryDetails"}) or node
        if not isinstance(entry, dict):
            return None
        return OCEntryDetailsSchema(
            entry_number=self._extract_text(self._find_first_key(entry, {"EntryNumber"})),
            entry_text=self._extract_text(self._find_first_key(entry, {"EntryText"})),
            registration_date=self._parse_date(
                self._extract_text(self._find_first_key(entry, {"RegistrationDate"}))
            ),
            sub_register_code=self._extract_text(self._find_first_key(entry, {"SubRegisterCode"})),
            schedule_code=self._extract_text(self._find_first_key(entry, {"ScheduleCode"})),
            infills=self._coerce_dict(self._find_first_key(entry, {"Infills"})),
        )

    def _build_address_schema(self, value: Any) -> Optional[OCAddressSchema]:
        if not value:
            return None
        lines = self._extract_address_lines(value)
        return OCAddressSchema(address_lines=lines)

    def _extract_address_lines(self, value: Any) -> list[str]:
        lines: list[str] = []
        if isinstance(value, dict):
            if "AddressLine" in value:
                address_line = value["AddressLine"]
                if isinstance(address_line, list):
                    for item in address_line:
                        lines.extend(self._extract_address_lines(item))
                else:
                    lines.extend(self._extract_address_lines(address_line))
            if "Line" in value:
                raw = value["Line"]
                if isinstance(raw, list):
                    lines.extend([self._extract_text(item) or "" for item in raw])
                else:
                    text = self._extract_text(raw)
                    if text:
                        lines.append(text)
            for child in value.values():
                if isinstance(child, (dict, list)):
                    lines.extend(self._extract_address_lines(child))
        elif isinstance(value, list):
            for item in value:
                lines.extend(self._extract_address_lines(item))
        cleaned = [line for line in (line.strip() for line in lines if line) if line]
        # Preserve order while removing duplicates.
        return list(dict.fromkeys(cleaned))

    def _coerce_dict(self, value: Any) -> dict[str, Any]:
        return value if isinstance(value, dict) else {}

    def _coerce_list(self, value: Any) -> list[dict[str, Any]]:
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            return [value]
        return []

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

    def _extract_text(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            text = re.sub(r"\s+", " ", value).strip()
            return text or None
        if isinstance(value, dict):
            for key in ("#text", "Text", "Description", "Value", "Reference", "Line"):
                if key in value:
                    return self._extract_text(value[key])
            parts = [self._extract_text(item) for item in value.values()]
            joined = " ".join(part for part in parts if part)
            return joined or None
        if isinstance(value, list):
            parts = [self._extract_text(item) for item in value]
            joined = " ".join(part for part in parts if part)
            return joined or None
        return str(value)

    def _parse_date(self, value: Optional[str]) -> Optional[date]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value).date()
        except ValueError:
            try:
                return datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError:
                return None

    def _parse_datetime(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        cleaned = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(cleaned)
        except ValueError:
            return None

    def _extract_pdf_bytes(self, raw_xml: str) -> Optional[bytes]:
        parsed = xmltodict.parse(raw_xml)
        attachment = self._find_first_key(parsed, {"Attachment"}) or {}
        embedded = self._extract_text(
            self._find_first_key(
                attachment,
                {"EmbeddedFileBinaryObject", "BinaryData", "DocumentBinary"},
            )
        )
        if not embedded:
            return None
        try:
            return base64.b64decode(embedded)
        except Exception:
            return None

    def _write_pdf_to_disk(self, pdf_bytes: bytes, filename: str) -> Path:
        self._pdf_root.mkdir(parents=True, exist_ok=True)
        target = self._pdf_root / filename
        target.write_bytes(pdf_bytes)
        return target
