"""API-level Stage 2 verification test using real LandRegistryClient.

This focuses ONLY on the second verification step:
- Seeds a set of realistic PPD-style records into the test DB.
- Calls `/api/v1/verification/verify` with all of them.
- Asserts the API responds successfully and logs the full payload.

The actual verification outcome (`confirmed_fraud` / `not_fraud` / `error`)
depends on your real Land Registry integration and environment.
"""

import json
from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.fraud_match import FraudMatch
from src.models.property_listing import PropertyListing


# ppd_transaction_id is used as the OOV MessageId. The BG test stub only
# returns canned responses for specific MessageId values (the stub scenario keys).
# All 9 keys below trigger a TypeCode=30 Result response from the stub.
PPD_TEST_RECORDS = [
    {
        "stub_key": "eoov-fm-1",    # full match: surname+forename+middle MATCH, current owner
        "price": 451225,
        "day": 31,
        "month": 7,
        "year": 2025,
        "postcode": "PL1 1QQ",
        "address": "24 DOVEDALE ROAD PLYMOUTH",
    },
    {
        "stub_key": "eoov-snm-1",   # surname MATCH, forename NO_MATCH
        "price": 485000,
        "day": 20,
        "month": 1,
        "year": 2025,
        "postcode": "PL1 1QQ",
        "address": "24 DOVEDALE ROAD PLYMOUTH",
    },
    {
        "stub_key": "eoov-snpm-1",  # surname PARTIAL_MATCH, forename NO_MATCH
        "price": 380000,
        "day": 20,
        "month": 3,
        "year": 2025,
        "postcode": "PL1 1QQ",
        "address": "24 DOVEDALE ROAD PLYMOUTH",
    },
    {
        "stub_key": "eoov-fnpm-1",  # surname MATCH, forename PARTIAL_MATCH
        "price": 730000,
        "day": 27,
        "month": 3,
        "year": 2025,
        "postcode": "PL1 1QQ",
        "address": "24 DOVEDALE ROAD PLYMOUTH",
    },
    {
        "stub_key": "eoov-mnm-1",   # surname+forename MATCH, middle NO_MATCH, historical
        "price": 325000,
        "day": 28,
        "month": 3,
        "year": 2025,
        "postcode": "PL1 1QQ",
        "address": "24 DOVEDALE ROAD PLYMOUTH",
    },
    {
        "stub_key": "eoov-nsm-1",   # surname NO_MATCH, string MATCH
        "price": 250000,
        "day": 14,
        "month": 3,
        "year": 2025,
        "postcode": "PL1 1QQ",
        "address": "24 DOVEDALE ROAD PLYMOUTH",
    },
    {
        "stub_key": "eoov-nm-1",    # surname NO_MATCH, string NO_MATCH -> no_fraud
        "price": 925000,
        "day": 28,
        "month": 7,
        "year": 2025,
        "postcode": "PL1 1QQ",
        "address": "24 DOVEDALE ROAD PLYMOUTH",
    },
    {
        "stub_key": "eoov-hm-1",    # surname+forename+middle MATCH, historical owner
        "price": 600100,
        "day": 21,
        "month": 3,
        "year": 2025,
        "postcode": "PL1 1QQ",
        "address": "24 DOVEDALE ROAD PLYMOUTH",
    },
    {
        "stub_key": "eoov-nam-1",   # rejection: bg.properties.nopropertyfound
        "price": 470000,
        "day": 11,
        "month": 3,
        "year": 2025,
        "postcode": "PL1 1QQ",
        "address": "24 DOVEDALE ROAD PLYMOUTH",
    },
]


@pytest.mark.asyncio
async def test_stage2_verification_real_land_registry(
    api_client,
    db_session: AsyncSession,
) -> None:
    """End-to-end Stage 2 test hitting the real LandRegistryClient."""
    match_ids: list[str] = []

    # Seed realistic property listings and fraud matches
    for idx, rec in enumerate(PPD_TEST_RECORDS, start=1):
        listing = PropertyListing(
            agency_id=f"agency-{idx}",
            address=rec["address"],
            normalized_address=rec["address"].upper(),
            postcode=rec["postcode"],
            client_name=f"Test Client {idx}",
            status="active",
            created_at=datetime.now(timezone.utc),
        )
        db_session.add(listing)
        await db_session.flush()

        fraud_match = FraudMatch(
            property_listing_id=listing.id,
            ppd_transaction_id=rec["stub_key"],
            ppd_price=rec["price"],
            ppd_transfer_date=datetime(
                rec["year"], rec["month"], rec["day"], tzinfo=timezone.utc
            ),
            ppd_postcode=rec["postcode"],
            ppd_full_address=rec["address"],
            confidence_score=95.0,
            address_similarity=95.0,
            risk_level="HIGH",
            detected_at=datetime.now(timezone.utc),
        )
        db_session.add(fraud_match)
        await db_session.flush()
        match_ids.append(fraud_match.id)

    await db_session.commit()

    # Call the real Stage 2 verification endpoint
    response = await api_client.post(
        "/api/v1/verification/verify",
        json={"match_ids": match_ids},
    )

    assert response.status_code == 200
    data = response.json()

    # Log full response so you can inspect real HMLR behaviour
    print("\n--- POST /api/v1/verification/verify (real LandRegistryClient) ---")
    print(json.dumps(data, indent=2, default=str))

    # Minimal structural checks: just ensure the second step ran end-to-end
    assert data["total_verified"] == len(match_ids)
    assert len(data["results"]) == len(match_ids)

    # Each result should correspond to one of our seeded matches
    returned_ids = {r["match_id"] for r in data["results"]}
    assert returned_ids == set(match_ids)

"""API-level tests for the verification endpoints.

Uses the real LandRegistryClient (no mocks). It calls the live client config
(LAND_REGISTRY_API_URL / LAND_REGISTRY_API_KEY from env). When the Land Registry
API is a placeholder, results are errors; when integrated, you get real verification.
"""

import json
from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.fraud_match import FraudMatch
from src.models.property_listing import PropertyListing


@pytest.mark.asyncio
async def test_verify_matches_endpoint_with_real_land_registry_client(
    api_client,
    db_session: AsyncSession,
) -> None:
    """POST /verification/verify using the real LandRegistryClient (no mock)."""
    listing = PropertyListing(
        agency_id="agency-api-1",
        address="200 API Road",
        normalized_address="200 API ROAD",
        postcode="PL1 1QQ",
        client_name="API User",
        status="active",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(listing)
    await db_session.flush()

    fraud_match = FraudMatch(
        property_listing_id=listing.id,
        ppd_transaction_id="eoov-fm-1",
        ppd_price=400000,
        ppd_transfer_date=datetime(2024, 6, 1),
        ppd_postcode="PL1 1QQ",
        ppd_full_address="24 DOVEDALE ROAD PLYMOUTH",
        confidence_score=93.0,
        address_similarity=94.0,
        risk_level="HIGH",
        detected_at=datetime.now(timezone.utc),
    )
    db_session.add(fraud_match)
    await db_session.commit()

    response = await api_client.post(
        "/api/v1/verification/verify",
        json={"match_ids": [fraud_match.id]},
    )

    assert response.status_code == 200
    data = response.json()

    # Log real API result when running pytest
    print("\n--- POST /api/v1/verification/verify (real LandRegistryClient) ---")
    print(json.dumps(data, indent=2, default=str))

    # Response shape and counts reflect what the real client returned
    assert data["total_verified"] == 1
    assert len(data["results"]) == 1
    assert data["results"][0]["match_id"] == fraud_match.id
    assert data["results"][0]["property_address"] == listing.address
    assert data["results"][0]["client_name"] == listing.client_name

    # Counts must sum to total_verified
    assert (
        data["confirmed_fraud_count"]
        + data["not_fraud_count"]
        + data["error_count"]
        == data["total_verified"]
    )

    # With placeholder client we get error; with real HMLR API you may get confirmed_fraud or not_fraud
    result = data["results"][0]
    assert result["verification_status"] in ("confirmed_fraud", "not_fraud", "error")
    assert result["verified_at"] is not None


@pytest.mark.asyncio
async def test_get_verification_status_endpoint(
    api_client,
    db_session: AsyncSession,
) -> None:
    """Exercise the /verification/status/{match_id} endpoint."""
    listing = PropertyListing(
        agency_id="agency-api-2",
        address="300 Status Street",
        normalized_address="300 STATUS STREET",
        postcode="ST1 1TS",
        client_name="Status User",
        status="active",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(listing)
    await db_session.flush()

    fraud_match = FraudMatch(
        property_listing_id=listing.id,
        ppd_transaction_id="api-tx-2",
        ppd_price=275000,
        ppd_transfer_date=datetime(2024, 7, 1),
        ppd_postcode="ST1 1TS",
        ppd_full_address="300 Status Street",
        confidence_score=80.0,
        address_similarity=81.0,
        risk_level="MEDIUM",
        detected_at=datetime.now(timezone.utc),
        verification_status="suspicious",
    )
    db_session.add(fraud_match)
    await db_session.commit()

    response = await api_client.get(
        f"/api/v1/verification/status/{fraud_match.id}",
    )

    assert response.status_code == 200
    data = response.json()

    # Log API result so it appears when running pytest
    print("\n--- GET /api/v1/verification/status/{match_id} response ---")
    print(json.dumps(data, indent=2, default=str))

    assert data["match_id"] == fraud_match.id
    assert data["property_address"] == listing.address
    assert data["client_name"] == listing.client_name
    assert data["verification_status"] == "suspicious"

