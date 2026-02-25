"""
Agency service for business logic and database operations.
"""

import uuid
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.agency import Agency
from src.models.property_listing import PropertyListing
from src.models.fraud_match import FraudMatch
from src.schemas.agency import AgencyCreate, AgencyUpdate


class AgencyService:
    """Service class for agency-related operations."""

    @staticmethod
    async def create_agency(db: AsyncSession, agency_data: AgencyCreate) -> Agency:
        """
        Create a new agency.

        Args:
            db: Database session
            agency_data: Agency creation data

        Returns:
            Created agency instance
        """
        agency = Agency(
            id=str(uuid.uuid4()),
            name=agency_data.name,
        )
        db.add(agency)
        await db.commit()
        await db.refresh(agency)
        return agency

    @staticmethod
    async def get_agency(db: AsyncSession, agency_id: str) -> Optional[Agency]:
        """
        Get agency by ID.

        Args:
            db: Database session
            agency_id: Agency identifier

        Returns:
            Agency instance or None if not found
        """
        stmt = select(Agency).where(Agency.id == agency_id)
        result = await db.execute(stmt)
        return result.scalar_one_or_none()

    @staticmethod
    async def get_agencies(
        db: AsyncSession, skip: int = 0, limit: int = 100
    ) -> tuple[list[Agency], int]:
        """
        Get paginated list of agencies.

        Args:
            db: Database session
            skip: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            Tuple of (agencies list, total count)
        """
        # Get total count
        count_stmt = select(func.count()).select_from(Agency)
        total_result = await db.execute(count_stmt)
        total = total_result.scalar()

        # Get paginated agencies
        stmt = (
            select(Agency).offset(skip).limit(limit).order_by(Agency.created_at.desc())
        )
        result = await db.execute(stmt)
        agencies = result.scalars().all()

        return list(agencies), total

    @staticmethod
    async def update_agency(
        db: AsyncSession, agency_id: str, agency_data: AgencyUpdate
    ) -> Optional[Agency]:
        """
        Update an existing agency.

        Args:
            db: Database session
            agency_id: Agency identifier
            agency_data: Agency update data

        Returns:
            Updated agency instance or None if not found
        """
        stmt = select(Agency).where(Agency.id == agency_id)
        result = await db.execute(stmt)
        agency = result.scalar_one_or_none()

        if not agency:
            return None

        # Update only provided fields
        if agency_data.name is not None:
            agency.name = agency_data.name

        await db.commit()
        await db.refresh(agency)
        return agency

    @staticmethod
    async def update_agency_alto(
        db: AsyncSession, agency_id: str, alto_agency_ref: Optional[str]
    ) -> Optional[Agency]:
        """
        Update agency Alto settings.

        Args:
            db: Database session
            agency_id: Agency identifier
            alto_agency_ref: New Alto Agency Ref (or None)

        Returns:
            Updated agency instance or None if not found
        """
        stmt = select(Agency).where(Agency.id == agency_id)
        result = await db.execute(stmt)
        agency = result.scalar_one_or_none()

        if not agency:
            return None

        agency.alto_agency_ref = alto_agency_ref

        await db.commit()
        await db.refresh(agency)
        return agency

    @staticmethod
    async def delete_agency(db: AsyncSession, agency_id: str) -> bool:
        """
        Delete an agency.

        Args:
            db: Database session
            agency_id: Agency identifier

        Returns:
            True if deleted, False if not found
        """
        stmt = select(Agency).where(Agency.id == agency_id)
        result = await db.execute(stmt)
        agency = result.scalar_one_or_none()

        if not agency:
            return False

        await db.delete(agency)
        await db.commit()
        return True

    @staticmethod
    async def get_agency_stats(db: AsyncSession, agency_id: str) -> dict:
        """
        Get statistics for an agency.
        """
        # Total listings
        listings_stmt = (
            select(func.count())
            .select_from(PropertyListing)
            .where(PropertyListing.agency_id == agency_id)
        )
        listings_result = await db.execute(listings_stmt)
        total_listings = listings_result.scalar() or 0

        # Suspicious matches
        suspicious_stmt = (
            select(func.count())
            .select_from(FraudMatch)
            .join(PropertyListing)
            .where(
                PropertyListing.agency_id == agency_id,
                FraudMatch.verification_status == "suspicious",
            )
        )
        suspicious_result = await db.execute(suspicious_stmt)
        suspicious_matches = suspicious_result.scalar() or 0

        # Confirmed fraud
        fraud_stmt = (
            select(FraudMatch)
            .join(PropertyListing)
            .where(
                PropertyListing.agency_id == agency_id,
                FraudMatch.verification_status == "confirmed_fraud",
            )
        )
        fraud_result = await db.execute(fraud_stmt)
        fraud_matches = fraud_result.scalars().all()
        confirmed_fraud_count = len(fraud_matches)

        # Potential savings (1.5% of property value)
        potential_savings = sum(
            (match.ppd_price or 0) * 0.015 for match in fraud_matches
        )

        return {
            "total_listings": total_listings,
            "suspicious_matches": suspicious_matches,
            "confirmed_fraud": confirmed_fraud_count,
            "potential_savings": potential_savings,
        }
