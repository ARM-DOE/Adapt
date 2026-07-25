# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Per-method segmentation parameter schemas.

One frozen model per segmentation method, holding exactly the scientific
parameters Adapt passes to the underlying algorithm. Field names match the
target function's keyword arguments exactly, so a model can be ``model_dump()``ed
and splatted straight into the call. Defaults equal the algorithms' own current
defaults — behaviour is unchanged, but the values are now owned, resolved, and
recorded by Adapt instead of relying on the library's implicit defaults.

Defined once and imported by both ParamConfig (expert defaults) and
InternalConfig (authoritative runtime) so the two layers cannot drift.

Derived arguments (dx, dy, work level / cappi level, reflectivity field name)
are NOT parameters here — they are computed from the grid and z-level at call
time. Output-class encodings (e.g. the integer codes for convective/stratiform)
are also excluded: the segmenter's mask relies on the algorithms' default codes.
"""

from typing import Literal

from pydantic import Field

from adapt.configuration.schemas.base import AdaptBaseModel


class ThresholdParams(AdaptBaseModel):
    """Fixed reflectivity-threshold method."""

    threshold: float = Field(
        30.0, description="Reflectivity threshold in dBZ; cells have reflectivity above it"
    )


class ConvStratRautParams(AdaptBaseModel):
    """Raut et al. wavelet convective/stratiform classification (pyart conv_strat_raut)."""

    zr_a: float = Field(200.0, description="Z-R relation coefficient a in Z = a*R^b")
    zr_b: float = Field(1.6, description="Z-R relation exponent b in Z = a*R^b")
    core_wt_threshold: float = Field(
        5.0, description="Wavelet threshold separating convective cores from mixed"
    )
    conv_wt_threshold: float = Field(
        1.5, description="Wavelet threshold separating all convection from stratiform"
    )
    conv_scale_km: float = Field(
        25.0, description="Approx. scale break (km) between convective and stratiform scales"
    )
    min_reflectivity: float = Field(
        5.0, description="Minimum reflectivity (dBZ); below this is unclassified"
    )
    conv_min_refl: float = Field(
        25.0, description="Reflectivity (dBZ) below which points are never convective"
    )
    conv_core_threshold: float = Field(
        42.0, description="Reflectivity (dBZ) above which points are always convective cores"
    )


class ConvStratYuterParams(AdaptBaseModel):
    """Yuter/Powell convective/stratiform classification (pyart conv_strat_yuter)."""

    always_core_thres: float = Field(
        42.0, description="Reflectivity (dBZ) always classified as convective"
    )
    bkg_rad_km: float = Field(
        11.0, description="Radius (km) for the background reflectivity average"
    )
    use_cosine: bool = Field(
        True, description="Use a cosine convective-threshold scheme vs a scalar difference"
    )
    max_diff: float = Field(
        5.0, description="Maximum difference (dBZ) above background for the cosine scheme"
    )
    zero_diff_cos_val: float = Field(
        55.0, description="Reflectivity (dBZ) where the cosine difference reaches zero"
    )
    scalar_diff: float = Field(
        1.5, description="Scalar difference (dBZ) above background when not using cosine"
    )
    use_addition: bool = Field(
        True, description="Add the scalar/cosine difference to the background value"
    )
    calc_thres: float = Field(
        0.75, description="Minimum fraction of valid points required in a background window"
    )
    weak_echo_thres: float = Field(
        5.0, description="Reflectivity (dBZ) below which echo is considered weak"
    )
    min_dBZ_used: float = Field(
        5.0, description="Minimum reflectivity (dBZ) considered in the classification"
    )
    dB_averaging: bool = Field(
        True, description="Average reflectivity in dB (True for radar dBZ input)"
    )
    remove_small_objects: bool = Field(
        True, description="Remove convective features below min_km2_size"
    )
    min_km2_size: float = Field(10.0, description="Minimum convective feature size (km^2)")
    val_for_max_conv_rad: float = Field(
        30.0, description="Reflectivity (dBZ) at which the convective radius is maximal"
    )
    max_conv_rad_km: float = Field(5.0, description="Maximum convective radius (km)")


class FeatureDetectionParams(AdaptBaseModel):
    """Generalized feature detection (pyart feature_detection)."""

    always_core_thres: float = Field(42.0, description="Value always classified as a feature")
    bkg_rad_km: float = Field(11.0, description="Radius (km) for the background average")
    use_cosine: bool = Field(
        True, description="Use a cosine feature-threshold scheme vs a scalar difference"
    )
    max_diff: float = Field(
        5.0, description="Maximum difference above background for the cosine scheme"
    )
    zero_diff_cos_val: float = Field(
        55.0, description="Value where the cosine difference reaches zero"
    )
    scalar_diff: float = Field(
        1.5, description="Scalar difference above background when not using cosine"
    )
    use_addition: bool = Field(
        True, description="Add the scalar/cosine difference to the background value"
    )
    calc_thres: float = Field(
        0.75, description="Minimum fraction of valid points required in a background window"
    )
    weak_echo_thres: float = Field(5.0, description="Value below which echo is considered weak")
    min_val_used: float = Field(5.0, description="Minimum field value considered in the detection")
    dB_averaging: bool = Field(
        True, description="Average the field in dB (True for radar dBZ input)"
    )
    remove_small_objects: bool = Field(True, description="Remove features below min_km2_size")
    min_km2_size: float = Field(10.0, description="Minimum feature size (km^2)")
    binary_close: bool = Field(False, description="Apply binary closing to the feature mask")
    val_for_max_rad: float = Field(
        30.0, description="Field value at which the feature radius is maximal"
    )
    max_rad_km: float = Field(5.0, description="Maximum feature radius (km)")


class SteinerConvStratParams(AdaptBaseModel):
    """Steiner et al. 1995 convective/stratiform classification (pyart steiner_conv_strat)."""

    intense: float = Field(42.0, description="Reflectivity (dBZ) always classified as convective")
    peak_relation: Literal["default", "sgp"] = Field(
        "default", description="Peakedness relation for convective centres"
    )
    area_relation: Literal["small", "medium", "large", "sgp"] = Field(
        "medium", description="Convective-radius area relation"
    )
    bkg_rad: float = Field(
        11000.0, description="Radius (m) for the background reflectivity average"
    )
    use_intense: bool = Field(
        True, description="Always classify points above `intense` as convective"
    )
