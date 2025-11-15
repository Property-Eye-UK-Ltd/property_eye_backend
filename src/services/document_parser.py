"""
Document parser service for agency listing files.

Supports CSV, Excel, and PDF formats with field mapping capabilities.
"""

from pathlib import Path
from typing import Dict

import pandas as pd

from src.utils.constants import config


class DocumentParser:
    """
    Service for parsing agency documents in multiple formats.

    Supports CSV, Excel (.xlsx, .xls), and PDF formats with
    configurable field mapping.
    """

    def __init__(self):
        """Initialize the document parser."""
        self.required_fields = config.REQUIRED_FIELDS

    async def parse(
        self, file_path: str, file_type: str, field_mapping: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Parse document and return normalized DataFrame.

        Args:
            file_path: Path to the document file
            file_type: File extension (.csv, .xlsx, .xls, .pdf)
            field_mapping: Dictionary mapping agency columns to system fields

        Returns:
            DataFrame with mapped and validated fields

        Raises:
            ValueError: If file type is unsupported or validation fails
            FileNotFoundError: If file doesn't exist
        """
        # Validate file exists
        if not Path(file_path).exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Parse based on file type
        if file_type.lower() == ".csv":
            df = self._parse_csv(file_path)
        elif file_type.lower() in [".xlsx", ".xls"]:
            df = self._parse_excel(file_path)
        elif file_type.lower() == ".pdf":
            df = self._parse_pdf(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        # Apply field mapping
        df = self._apply_field_mapping(df, field_mapping)

        # Validate required fields
        self._validate_required_fields(df)

        return df

    def _parse_csv(self, file_path: str) -> pd.DataFrame:
        """
        Parse CSV file using pandas.

        Args:
            file_path: Path to CSV file

        Returns:
            DataFrame with parsed data
        """
        try:
            df = pd.read_csv(file_path)
            return df
        except Exception as e:
            raise ValueError(f"Failed to parse CSV file: {str(e)}")

    def _parse_excel(self, file_path: str) -> pd.DataFrame:
        """
        Parse Excel file using pandas with openpyxl engine.

        Args:
            file_path: Path to Excel file

        Returns:
            DataFrame with parsed data
        """
        try:
            # Use openpyxl engine for .xlsx files
            df = pd.read_excel(file_path, engine="openpyxl")
            return df
        except Exception as e:
            raise ValueError(f"Failed to parse Excel file: {str(e)}")

    def _parse_pdf(self, file_path: str) -> pd.DataFrame:
        """
        Parse PDF file using pdfplumber.

        TODO: Implement PDF parsing with pdfplumber for table extraction.
        This is a lower priority feature and should be implemented last.

        Args:
            file_path: Path to PDF file

        Returns:
            DataFrame with parsed data

        Raises:
            NotImplementedError: PDF parsing not yet implemented
        """
        raise NotImplementedError(
            "PDF parsing is not yet implemented. "
            "Please use CSV or Excel format for now."
        )

    def _apply_field_mapping(
        self, df: pd.DataFrame, field_mapping: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Rename DataFrame columns based on field mapping.

        Args:
            df: Original DataFrame
            field_mapping: Dictionary mapping agency columns to system fields

        Returns:
            DataFrame with renamed columns

        Raises:
            ValueError: If mapped columns don't exist in DataFrame
        """
        # Validate that all mapped columns exist in the DataFrame
        missing_columns = [col for col in field_mapping.keys() if col not in df.columns]

        if missing_columns:
            raise ValueError(
                f"The following mapped columns are missing from the document: "
                f"{', '.join(missing_columns)}"
            )

        # Rename columns according to mapping
        df = df.rename(columns=field_mapping)

        return df

    def _validate_required_fields(self, df: pd.DataFrame) -> None:
        """
        Validate that all required fields are present in the DataFrame.

        Args:
            df: DataFrame to validate

        Raises:
            ValueError: If required fields are missing
        """
        missing_fields = [
            field for field in self.required_fields if field not in df.columns
        ]

        if missing_fields:
            raise ValueError(
                f"The following required fields are missing after mapping: "
                f"{', '.join(missing_fields)}. "
                f"Please ensure your field_mapping includes all required fields: "
                f"{', '.join(self.required_fields)}"
            )
