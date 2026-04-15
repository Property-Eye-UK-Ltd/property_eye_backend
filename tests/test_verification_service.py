"""Unit tests for the VerificationService and related Land Registry flows."""

import json
from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.fraud_match import FraudMatch
from src.models.property_listing import PropertyListing
from src.services.land_registry_client import OwnershipVerificationResult
from src.services.verification_service import VerificationService


class FakeLandRegistryClientSuccess:
    """Fake Land Registry client that always confirms ownership."""

    async def verify_ownership(
        self,
        property_address: str,
        postcode: str,
        expected_owner_name: str,
        message_id: str = None,
        **kwargs,
    ) -> OwnershipVerificationResult:
        """Return a successful ownership verification result."""
        return OwnershipVerificationResult(
            owner_name=expected_owner_name,
            verification_status="ok",
            error_message=None,
            raw_response={"owner_name": expected_owner_name},
        )


class FakeLandRegistryClientMismatch:
    """Fake Land Registry client that always returns a different owner."""

    async def verify_ownership(
        self,
        property_address: str,
        postcode: str,
        expected_owner_name: str,
        message_id: str = None,
        **kwargs,
    ) -> OwnershipVerificationResult:
        """Return a successful response with a mismatching owner name."""
        return OwnershipVerificationResult(
            owner_name="Different Owner",
            verification_status="ok",
            error_message=None,
            raw_response={"owner_name": "Different Owner"},
        )


class FakeLandRegistryClientError:
    """Fake Land Registry client that always reports an API error."""

    async def verify_ownership(
        self,
        property_address: str,
        postcode: str,
        expected_owner_name: str,
        message_id: str = None,
        **kwargs,
    ) -> OwnershipVerificationResult:
        """Return an error verification result."""
        return OwnershipVerificationResult(
            owner_name=None,
            verification_status="error",
            error_message="Simulated API error",
            raw_response=None,
        )


class FakeLandRegistryClientException:
    """Fake Land Registry client that raises an unexpected exception."""

    async def verify_ownership(
        self,
        property_address: str,
        postcode: str,
        expected_owner_name: str,
        message_id: str = None,
        **kwargs,
    ) -> OwnershipVerificationResult:  # type: ignore[override]
        """Raise a runtime error to simulate unexpected failure."""
        raise RuntimeError("Simulated unexpected failure")


@pytest.mark.asyncio
async def test_verify_single_match_confirmed_fraud(db_session: AsyncSession) -> None:
    """Verify that a matching owner name marks the match as confirmed fraud."""
    listing = PropertyListing(
        agency_id="agency-1",
        address="123 High Street",
        normalized_address="123 HIGH STREET",
        postcode="AB1 2CD",
        client_name="John Smith",
        status="active",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(listing)
    await db_session.flush()

    fraud_match = FraudMatch(
        property_listing_id=listing.id,
        ppd_transaction_id="tx-1",
        ppd_price=350000,
        ppd_transfer_date=datetime(2024, 1, 1),
        ppd_postcode="AB1 2CD",
        ppd_full_address="123 High Street",
        confidence_score=95.0,
        address_similarity=96.0,
        risk_level="HIGH",
        detected_at=datetime.now(timezone.utc),
    )
    db_session.add(fraud_match)
    await db_session.commit()

    service = VerificationService(land_registry_client=FakeLandRegistryClientSuccess())

    result = await service.verify_single_match(fraud_match.id, db_session)

    refreshed = await db_session.get(FraudMatch, fraud_match.id)

    assert result.verification_status == "confirmed_fraud"
    assert result.is_confirmed_fraud is True
    assert result.verified_owner_name == listing.client_name
    assert refreshed is not None and refreshed.verification_status == "confirmed_fraud"


@pytest.mark.asyncio
async def test_verify_single_match_not_fraud(db_session: AsyncSession) -> None:
    """Verify that a non-matching owner name marks the match as not_fraud."""
    listing = PropertyListing(
        agency_id="agency-2",
        address="45 Market Road",
        normalized_address="45 MARKET ROAD",
        postcode="ZX9 9ZZ",
        client_name="Alice Example",
        status="active",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(listing)
    await db_session.flush()

    fraud_match = FraudMatch(
        property_listing_id=listing.id,
        ppd_transaction_id="tx-2",
        ppd_price=500000,
        ppd_transfer_date=datetime(2024, 2, 1),
        ppd_postcode="ZX9 9ZZ",
        ppd_full_address="45 Market Road",
        confidence_score=90.0,
        address_similarity=90.0,
        risk_level="MEDIUM",
        detected_at=datetime.now(timezone.utc),
    )
    db_session.add(fraud_match)
    await db_session.commit()

    service = VerificationService(land_registry_client=FakeLandRegistryClientMismatch())

    result = await service.verify_single_match(fraud_match.id, db_session)

    refreshed = await db_session.get(FraudMatch, fraud_match.id)

    assert result.verification_status == "not_fraud"
    assert result.is_confirmed_fraud is False
    assert refreshed is not None and refreshed.verification_status == "not_fraud"


@pytest.mark.asyncio
async def test_verify_single_match_api_error(db_session: AsyncSession) -> None:
    """Verify that an API error sets verification_status to error."""
    listing = PropertyListing(
        agency_id="agency-3",
        address="10 Test Lane",
        normalized_address="10 TEST LANE",
        postcode="QQ1 1QQ",
        client_name="Bob Tester",
        status="active",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(listing)
    await db_session.flush()

    fraud_match = FraudMatch(
        property_listing_id=listing.id,
        ppd_transaction_id="tx-3",
        ppd_price=250000,
        ppd_transfer_date=datetime(2024, 3, 1),
        ppd_postcode="QQ1 1QQ",
        ppd_full_address="10 Test Lane",
        confidence_score=88.0,
        address_similarity=87.0,
        risk_level="LOW",
        detected_at=datetime.now(timezone.utc),
    )
    db_session.add(fraud_match)
    await db_session.commit()

    service = VerificationService(land_registry_client=FakeLandRegistryClientError())

    result = await service.verify_single_match(fraud_match.id, db_session)

    refreshed = await db_session.get(FraudMatch, fraud_match.id)

    assert result.verification_status == "error"
    assert result.is_confirmed_fraud is False
    assert "Simulated API error" in (result.error_message or "")
    assert refreshed is not None and refreshed.verification_status == "error"


@pytest.mark.asyncio
async def test_verify_single_match_exception_handling(db_session: AsyncSession) -> None:
    """Verify that unexpected exceptions are converted into error results."""
    listing = PropertyListing(
        agency_id="agency-4",
        address="99 Edge Case Way",
        normalized_address="99 EDGE CASE WAY",
        postcode="EC1 1EC",
        client_name="Carol Edge",
        status="active",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(listing)
    await db_session.flush()

    fraud_match = FraudMatch(
        property_listing_id=listing.id,
        ppd_transaction_id="tx-4",
        ppd_price=150000,
        ppd_transfer_date=datetime(2024, 4, 1),
        ppd_postcode="EC1 1EC",
        ppd_full_address="99 Edge Case Way",
        confidence_score=80.0,
        address_similarity=82.0,
        risk_level="MEDIUM",
        detected_at=datetime.now(timezone.utc),
    )
    db_session.add(fraud_match)
    await db_session.commit()

    service = VerificationService(land_registry_client=FakeLandRegistryClientException())

    result = await service.verify_single_match(fraud_match.id, db_session)

    refreshed = await db_session.get(FraudMatch, fraud_match.id)

    assert result.verification_status == "error"
    assert result.is_confirmed_fraud is False
    assert "Simulated unexpected failure" in (result.error_message or "")
    assert refreshed is not None and refreshed.verification_status == "error"


@pytest.mark.asyncio
async def test_verify_suspicious_matches_summary_counts(db_session: AsyncSession) -> None:
    """Verify that verify_suspicious_matches aggregates summary counts correctly."""
    listing = PropertyListing(
        agency_id="agency-5",
        address="1 Summary Street",
        normalized_address="1 SUMMARY STREET",
        postcode=None,
        client_name="Summary User",
        status="active",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(listing)
    await db_session.flush()

    match_confirmed = FraudMatch(
        property_listing_id=listing.id,
        ppd_transaction_id="tx-5a",
        ppd_price=300000,
        ppd_transfer_date=datetime(2024, 5, 1),
        ppd_postcode="PC-CONFIRMED",
        ppd_full_address="1 Summary Street",
        confidence_score=92.0,
        address_similarity=93.0,
        risk_level="HIGH",
        detected_at=datetime.now(timezone.utc),
    )
    match_not_fraud = FraudMatch(
        property_listing_id=listing.id,
        ppd_transaction_id="tx-5b",
        ppd_price=310000,
        ppd_transfer_date=datetime(2024, 5, 2),
        ppd_postcode="PC-NOT-FRAUD",
        ppd_full_address="1 Summary Street",
        confidence_score=85.0,
        address_similarity=84.0,
        risk_level="MEDIUM",
        detected_at=datetime.now(timezone.utc),
    )
    match_error = FraudMatch(
        property_listing_id=listing.id,
        ppd_transaction_id="tx-5c",
        ppd_price=320000,
        ppd_transfer_date=datetime(2024, 5, 3),
        ppd_postcode="PC-ERROR",
        ppd_full_address="1 Summary Street",
        confidence_score=70.0,
        address_similarity=72.0,
        risk_level="LOW",
        detected_at=datetime.now(timezone.utc),
    )

    db_session.add_all([match_confirmed, match_not_fraud, match_error])
    await db_session.commit()

    class FakeLandRegistryClientByPostcode:
        """Fake Land Registry client that varies response based on postcode."""

        async def verify_ownership(
            self,
            property_address: str,
            postcode: str,
            expected_owner_name: str,
            message_id: str = None,
            **kwargs,
        ) -> OwnershipVerificationResult:
            """Return different results depending on the supplied postcode."""
            if postcode == "PC-CONFIRMED":
                return OwnershipVerificationResult(
                    owner_name=expected_owner_name,
                    verification_status="ok",
                    error_message=None,
                    raw_response={"scenario": "confirmed"},
                )
            if postcode == "PC-NOT-FRAUD":
                return OwnershipVerificationResult(
                    owner_name="Unrelated Owner",
                    verification_status="ok",
                    error_message=None,
                    raw_response={"scenario": "not_fraud"},
                )
            return OwnershipVerificationResult(
                owner_name=None,
                verification_status="error",
                error_message="Scenario error",
                raw_response={"scenario": "error"},
            )

    service = VerificationService(
        land_registry_client=FakeLandRegistryClientByPostcode(),
    )

    summary = await service.verify_suspicious_matches(
        [match_confirmed.id, match_not_fraud.id, match_error.id],
        db_session,
    )

    # Log summary so it appears when running pytest
    print("\n--- VerificationSummary (verify_suspicious_matches) ---")
    print(json.dumps(summary.model_dump(), indent=2, default=str))

    assert summary.total_verified == 3
    assert summary.confirmed_fraud_count == 1
    assert summary.not_fraud_count == 1
    assert summary.error_count == 1
    assert len(summary.results) == 3
