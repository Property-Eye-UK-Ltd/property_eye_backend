"""
Address normalization service for UK addresses.

Provides methods to normalize and compare UK addresses for fraud detection.
"""

import re
from typing import Dict

from rapidfuzz import fuzz


class AddressNormalizer:
    """
    Service for normalizing and comparing UK addresses.

    Handles common UK address variations and provides fuzzy matching
    capabilities for fraud detection.
    """

    # Mapping of standard terms to their common abbreviations
    ABBREVIATION_MAP: Dict[str, list[str]] = {
        "STREET": ["ST", "STR", "STRT"],
        "ROAD": ["RD"],
        "AVENUE": ["AVE", "AV"],
        "DRIVE": ["DR", "DRV"],
        "LANE": ["LN"],
        "COURT": ["CT", "CRT"],
        "PLACE": ["PL"],
        "SQUARE": ["SQ", "SQR"],
        "TERRACE": ["TER", "TERR"],
        "GARDENS": ["GDNS", "GDN"],
        "CLOSE": ["CL"],
        "CRESCENT": ["CRES", "CR"],
        "GROVE": ["GR", "GRV"],
        "PARK": ["PK"],
        "WAY": ["WY"],
        "FLAT": ["FL", "APT", "APARTMENT"],
        "HOUSE": ["HSE"],
        "BUILDING": ["BLDG", "BLD"],
        "FLOOR": ["FLR"],
        "NORTH": ["N"],
        "SOUTH": ["S"],
        "EAST": ["E"],
        "WEST": ["W"],
    }

    def normalize(self, address: str, postcode: str = None) -> str:
        """
        Normalize a UK address for comparison.

        Args:
            address: The address string to normalize
            postcode: Optional postcode to append

        Returns:
            Normalized address string
        """
        if not address:
            return ""

        # Convert to uppercase
        normalized = address.upper()

        # Remove extra whitespace
        normalized = re.sub(r"\s+", " ", normalized).strip()

        # Remove common punctuation
        normalized = normalized.replace(",", " ")
        normalized = normalized.replace(".", " ")
        normalized = normalized.replace("-", " ")

        # Standardize abbreviations
        for standard, abbreviations in self.ABBREVIATION_MAP.items():
            for abbr in abbreviations:
                # Use word boundaries to avoid partial matches
                pattern = r"\b" + re.escape(abbr) + r"\b"
                normalized = re.sub(pattern, standard, normalized)

        # Remove extra whitespace again after replacements
        normalized = re.sub(r"\s+", " ", normalized).strip()

        # Append and format postcode if provided
        if postcode:
            formatted_postcode = self._format_postcode(postcode)
            if formatted_postcode and formatted_postcode not in normalized:
                normalized = f"{normalized} {formatted_postcode}"

        return normalized

    def _format_postcode(self, postcode: str) -> str:
        """
        Format UK postcode to standard format.

        UK postcodes follow the pattern: AA9A 9AA, A9A 9AA, A9 9AA, A99 9AA, AA9 9AA, AA99 9AA

        Args:
            postcode: Raw postcode string

        Returns:
            Formatted postcode (e.g., "SW1A 1AA")
        """
        if not postcode:
            return ""

        # Remove all whitespace and convert to uppercase
        postcode = postcode.replace(" ", "").upper()

        # UK postcode should be 5-7 characters
        if len(postcode) < 5 or len(postcode) > 7:
            return postcode

        # Insert space before last 3 characters (outward code + inward code)
        formatted = f"{postcode[:-3]} {postcode[-3:]}"

        return formatted

    def calculate_similarity(self, address1: str, address2: str) -> float:
        """
        Calculate similarity score between two addresses using fuzzy matching.

        Args:
            address1: First address string
            address2: Second address string

        Returns:
            Similarity score (0-100)
        """
        if not address1 or not address2:
            return 0.0

        # Normalize both addresses first
        norm1 = self.normalize(address1)
        norm2 = self.normalize(address2)

        # Use token sort ratio for better handling of word order differences
        similarity = fuzz.token_sort_ratio(norm1, norm2)

        return float(similarity)
