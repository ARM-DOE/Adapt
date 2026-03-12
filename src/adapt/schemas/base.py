"""Base Pydantic model with strict defaults for Adapt configs.

All Adapt config schemas inherit from this base to ensure consistent
validation behavior across parameter, user, CLI, and internal configs.
"""

from pydantic import BaseModel, ConfigDict


class AdaptBaseModel(BaseModel):
    """Base model for all Adapt configuration schemas.
    
    Enforces strict validation:
    - No extra fields allowed
    - Validates assignments after initialization
    - Uses Python mode (not JSON mode)
    - Forbids mutations after construction (frozen for internal config)
    """
    
    model_config = ConfigDict(
        extra='forbid',           # Reject unknown fields
        validate_assignment=True, # Validate on field mutation
        use_enum_values=True,     # Convert enums to values
        str_strip_whitespace=True,# Strip whitespace from strings
    )
