"""
Custom exception classes for domain-specific errors.
"""


class FraudDetectionError(Exception):
    """Base exception for fraud detection errors."""

    pass


class InvalidFieldMappingError(FraudDetectionError):
    """Raised when field mapping is invalid or incomplete."""

    pass


class PPDNotLoadedError(FraudDetectionError):
    """Raised when PPD data is not available for fraud detection."""

    pass


class DocumentParsingError(FraudDetectionError):
    """Raised when document parsing fails."""

    pass


class LandRegistryAPIError(FraudDetectionError):
    """Raised when Land Registry API calls fail."""

    pass
