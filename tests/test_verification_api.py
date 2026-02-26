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


PPD_TEST_RECORDS = [
    {
        "price": 451225,
        "day": 31,
        "month": 7,
        "year": 2025,
        "postcode": "N15 4AB",
        "address": "288 PHILIP LANE, LONDON N15 4AB",
    },
    {
        "price": 485000,
        "day": 20,
        "month": 1,
        "year": 2025,
        "postcode": "N4 4NR",
        "address": "FLAT 19 CONNAUGHT LODGE, CONNAUGHT ROAD, LONDON N4 4NR",
    },
    {
        "price": 380000,
        "day": 20,
        "month": 3,
        "year": 2025,
        "postcode": "N21 3AN",
        "address": "41A FERNLEIGH ROAD, LONDON N21 3AN",
    },
    {
        "price": 730000,
        "day": 27,
        "month": 3,
        "year": 2025,
        "postcode": "EN5 3LT",
        "address": "3 HAYDEN CLOSE, BARNET EN5 3LT",
    },
    {
        "price": 325000,
        "day": 28,
        "month": 3,
        "year": 2025,
        "postcode": "EN5 1HY",
        "address": "2 LANDER COURT, 48 LYONSDOWN ROAD, NEW BARNET EN5 1HY",
    },
    {
        "price": 250000,
        "day": 14,
        "month": 3,
        "year": 2025,
        "postcode": "UB7 7PQ",
        "address": "FLAT 6 BROOKLYN HOUSE, 22 THE GREEN, WEST DRAYTON UB7 7PQ",
    },
    {
        "price": 925000,
        "day": 28,
        "month": 7,
        "year": 2025,
        "postcode": "NW6 7TU",
        "address": "22A WINCHESTER AVENUE, LONDON NW6 7TU",
    },
    {
        "price": 600100,
        "day": 21,
        "month": 3,
        "year": 2025,
        "postcode": "E17 9LS",
        "address": "27 ADDISON ROAD, LONDON E17 9LS",
    },
    {
        "price": 470000,
        "day": 11,
        "month": 3,
        "year": 2025,
        "postcode": "EN2 8QJ",
        "address": "FLAT 14 OAKINGTON COURT, 38 THE RIDGEWAY, ENFIELD EN2 8QJ",
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
            ppd_transaction_id=f"ppd-{idx}",
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
        postcode="AP1 1PI",
        client_name="API User",
        status="active",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(listing)
    await db_session.flush()

    fraud_match = FraudMatch(
        property_listing_id=listing.id,
        ppd_transaction_id="api-tx-1",
        ppd_price=400000,
        ppd_transfer_date=datetime(2024, 6, 1),
        ppd_postcode="AP1 1PI",
        ppd_full_address="200 API Road",
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

