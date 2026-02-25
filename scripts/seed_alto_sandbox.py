import asyncio
import argparse
import csv
import random
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime

import httpx

# Import from the existing project to reuse auth
# Adjust the python path invocation if needed, e.g. run with `python -m scripts.seed_alto_sandbox`
from src.integrations.alto.auth import alto_auth_client
from src.core.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class FraudRecord:
    transaction_id: str
    price: int
    completion_date: str
    postcode: str
    address_line1: str
    locality: Optional[str]
    town: str
    district: Optional[str]
    county: str

    @classmethod
    def from_line(cls, line: str) -> Optional["FraudRecord"]:
        """
        Parses a raw line from fraud-records.csv.
        """
        line = line.strip()
        if not line:
            return None

        # 1. Extract Transaction ID: "{...}"
        match_uuid = re.search(r"^(\{[-0-9A-F]+\})", line)
        if not match_uuid:
            logger.warning(f"Skipping malformed line (no UUID): {line[:50]}...")
            return None

        transaction_id = match_uuid.group(1)
        remainder = line[len(transaction_id) :]

        # 2. Extract Price: digits immediately following UUID
        # Heuristic: look for the start of the date
        match_date = re.search(r"(\d{1,2}/\d{1,2}/\d{4}\s\d{1,2}:\d{2})", remainder)

        if match_date:
            date_str = match_date.group(1)
            date_start_idx = match_date.start()

            price_str = remainder[:date_start_idx].strip()
            if not price_str.isdigit():
                # Maybe it has some other chars?
                logger.warning(
                    f"Skipping malformed line (invalid price '{price_str}'): {line[:50]}..."
                )
                return None
            price = int(price_str)

            completion_date = date_str
            remainder = remainder[match_date.end() :]
        else:
            logger.warning(f"Skipping malformed line (no date found): {line[:50]}...")
            return None

        # 3. Extract Postcode:
        # Heuristic: Match ^([A-Z]{1,2}[0-9][0-9A-Z]?\s?[0-9][A-Z]{2})
        remainder = remainder.strip()

        # DEBUG
        # print(f"DEBUG: Remainder: '{remainder}'")

        match_pc = re.search(r"^([A-Z]{1,2}\d{1,2}[A-Z]?\s\d[A-Z]{2})", remainder)
        if not match_pc:
            # Try stricter/looser?
            logger.warning(
                f"Skipping malformed line (no postcode match in '{remainder[:20]}...'): {line[:50]}..."
            )
            return None

        postcode = match_pc.group(1)
        remainder = remainder[len(postcode) :]

        # 4. Parse Address Stack (Tabs)
        # Re-parsing strategy: Try splitting by TAB first as primary method.

        parts = line.split("\t")

        # DEBUG
        # print(f"DEBUG: Parts count: {len(parts)}")

        if len(parts) > 10:
            try:
                # 0: Transaction ID
                # 1: Price
                # 2: Date
                # 3: Postcode
                # 4: Type (D/S/T/F/O)
                # 5: Old/New (Y/N)
                # 6: Duration (F/L)
                # 7: PAON
                # 8: SAON
                # 9: Street
                # 10: Locality
                # 11: Town/City
                # 12: District
                # 13: County
                # 14: Category (A/B)
                # 15: Status (A/C)

                tid = parts[0]
                pr = int(parts[1])
                dt = parts[2]
                pc = parts[3]

                paon = parts[7]
                saon = parts[8]
                street = parts[9]
                locality = parts[10]
                town = parts[11]
                district = parts[12]
                county = parts[13]

                # Construct address line 1
                addr_parts = []
                if saon:
                    addr_parts.append(saon)
                if paon:
                    addr_parts.append(paon)
                if street:
                    addr_parts.append(street)
                line1 = " ".join(addr_parts)

                return cls(
                    transaction_id=tid,
                    price=pr,
                    completion_date=dt,
                    postcode=pc,
                    address_line1=line1,
                    locality=locality,
                    town=town,
                    district=district,
                    county=county,
                )
            except Exception as e:
                # logger.error(f"Error parsing tab-separated line: {e}")
                # Fall through to heuristic if needed, or fail.
                pass

        # Fallback Heuristics for Address if TSV failed
        # Remainder now contains address stack: "SNF49HIGH STREET..."
        # This part is hard without delimiters.
        # But if TSV failed, and we have tabs, we should trust TSV failure reason (e.g. malformed).

        return None


def generate_dummy_listing(index: int) -> Dict[str, Any]:
    """Generates a random valid listing payload for Alto."""
    prop_type = random.choice(["House", "Flat", "Bungalow"])
    bedrooms = random.randint(1, 4)
    price = random.randint(150, 800) * 1000

    cities = ["London", "Manchester", "Birmingham", "Leeds"]
    streets = ["Main St", "High St", "Church Rd", "Park Ave"]

    return {
        "status": "available",
        "category": "residential",
        "tenure": "freehold",
        "keyFeatures": ["Double Glazed", "Central Heating"],
        "description": f"A lovely {bedrooms} bedroom {prop_type.lower()}.",
        "summaryDescription": f"Dummy listing {index}",
        "address": {
            "name": str(random.randint(1, 99)),
            "street": random.choice(streets),
            "town": random.choice(cities),
            "postcode": f"SW1A {random.randint(1, 9)}AA",
            "country": "GB",
        },
        "price": {"amount": price, "currency": "GBP", "qualifier": "asking_price"},
        "bedrooms": bedrooms,
        "bathrooms": random.randint(1, 2),
        "branchid": settings.ALTO_BRANCH_ID
        if hasattr(settings, "ALTO_BRANCH_ID")
        else "YOUR_BRANCH_ID_HERE",
    }


def fraud_record_to_listing(record: FraudRecord) -> Dict[str, Any]:
    """Maps a fraud record to an Alto listing payload."""
    prop_type = "House"

    return {
        "status": "sold",
        "category": "residential",
        "tenure": "freehold",
        "description": "Historic transaction data import.",
        "summaryDescription": "Market data import",
        "address": {
            "name": "",
            "street": record.address_line1,
            "locality": record.locality,
            "town": record.town,
            "county": record.county,
            "postcode": record.postcode,
            "country": "GB",
        },
        "price": {"amount": record.price, "currency": "GBP", "qualifier": "sold_price"},
        "bedrooms": random.randint(2, 5),
        "bathrooms": 1,
    }


async def seed_alto(csv_path: str, count: int, fraud_count: int, dry_run: bool):
    """Main seeding logic."""
    if not Path(csv_path).exists():
        logger.error(f"CSV file not found: {csv_path}")
        return

    # 1. Load Fraud Records
    fraud_records = []
    with open(csv_path, "r", encoding="utf-8") as f:
        for line in f:
            rec = FraudRecord.from_line(line)
            if rec:
                fraud_records.append(rec)

    logger.info(f"Loaded {len(fraud_records)} valid fraud records.")

    if len(fraud_records) < fraud_count:
        logger.warning(
            f"Requested {fraud_count} fraud records but only found {len(fraud_records)}. Using all available."
        )
        fraud_count = len(fraud_records)

    # 2. Select Subset
    if fraud_records:
        selected_fraud = random.sample(
            fraud_records, min(len(fraud_records), fraud_count)
        )
    else:
        selected_fraud = []

    # 3. Build List
    total_listings = count
    dummy_count = total_listings - len(selected_fraud)

    payloads = []

    # Generate dummies
    for i in range(dummy_count):
        payloads.append({"type": "dummy", "data": generate_dummy_listing(i)})

    # Generate frauds
    for rec in selected_fraud:
        payloads.append(
            {
                "type": "fraud",
                "data": fraud_record_to_listing(rec),
                "source_id": rec.transaction_id,
            }
        )

    # Shuffle
    random.shuffle(payloads)

    logger.info(
        f"Prepared {len(payloads)} listings ({len(selected_fraud)} fraud, {dummy_count} dummy)."
    )

    # 4. Process
    success_count = 0

    # Auth
    headers = {}
    if not dry_run:
        try:
            token = await alto_auth_client.get_access_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            logger.info("Authenticated with Alto.")
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return

    api_url = f"{settings.alto_api_base_url}/properties"

    for idx, item in enumerate(payloads):
        payload = item["data"]

        if dry_run:
            logger.info(
                f"[DRY-RUN] Would create {item['type']} listing: {payload.get('address', {}).get('postcode')}"
            )
            success_count += 1
            continue

        # Real Call
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(api_url, json=payload, headers=headers)
                if response.status_code in (200, 201):
                    data = response.json()
                    prop_id = data.get("id") or data.get("propertyId")
                    logger.info(
                        f"Created {item['type']} listing ({idx + 1}/{total_listings}): ID={prop_id}"
                    )
                    success_count += 1
                else:
                    logger.error(
                        f"Failed to create {item['type']} listing: {response.status_code} - {response.text}"
                    )
            except Exception as e:
                logger.error(f"Exception calling Alto API: {e}")

    logger.info(
        f"Seeding complete. Successfully created {success_count}/{total_listings} listings."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed Alto Sandbox with listings")
    parser.add_argument("--csv", required=True, help="Path to fraud-records.csv")
    parser.add_argument(
        "--count", type=int, default=30, help="Total listings to create"
    )
    parser.add_argument(
        "--fraud-count",
        type=int,
        default=10,
        help="Number of fraud listings to include",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Do not make actual API calls"
    )

    args = parser.parse_args()

    asyncio.run(seed_alto(args.csv, args.count, args.fraud_count, args.dry_run))
