# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Lagrangian persistence: estimate cell motion and project future positions using optical flow.

The lagrangian extrapolation produces advection of the latest reflectivity field;
evolution of the advection field is not taken into account.


This module computes cell motion vectors from radar reflectivity patterns
using optical flow (Farneback algorithm). Motion vectors are then used to
project cell positions forward in time, providing future cell locations
for impact prediction and nowcasting.

Key capability:
- Registration: Project previous frame's cells to current position (backward projection)
- Future projections: Project current cells forward 1-5 steps (forward extrapolation)
- Optical flow computed on reflectivity fields, then applied to cell labels
- Generates both displacement vectors (heading_x, heading_y) and projected labels

Output enables cell tracking and motion-based warnings in operational systems.

@TODO: Handle tiny cells (Delaunay/qhull failures) with convex hull fallback
@TODO: Calibrate cell size thresholds from dataset analysis
"""

import logging
from datetime import datetime

import cv2
import numpy as np
import pandas as pd
import xarray as xr
from scipy.ndimage import binary_dilation
from scipy.spatial import Delaunay

__all__ = ["RadarCellProjector"]

logger = logging.getLogger(__name__)

# @TODO: When the cells are tiny, the qhull fails. We can use convex hull instead.
# @TODO: Decide the threshold by analyzing the cell sizes in the dataset.


class RadarCellProjector:
    """Compute cell motion vectors and project future cell positions.

    This class uses optical flow (Farneback algorithm) to estimate motion
    vectors from radar reflectivity patterns. Motion is then applied to
    segmented cell labels to generate:

    1. **Registration**: Project previous frame's cells to current position
       (validation: cells should match current segmentation if no change)

    2. **Future projections**: Extrapolate current cells 1-5 steps forward
       (nowcasting: where will cells be in next minutes?)

    3. **Motion fields**: Displacement vectors (heading_x, heading_y) for
       each pixel in pixels/frame

    The projector operates on 2D datasets (already sliced at fixed altitude
    by processor). Flow is computed on reflectivity patterns; labels are
    projected using computed flow vectors.

    Configuration
    ==============
    Config dict structure:

    - `method` : str, default "adapt_default"
        Currently only "adapt_default" (Farneback optical flow) supported.

    - `max_projection_steps` : int, default 1, max 10
        Number of future projections to compute (beyond registration).

    - `max_time_interval_minutes` : int, default 30
        Maximum time gap between consecutive frames. Skips processing
        if time gap exceeds this (motion model breaks down with large gaps).

    - `nan_fill_value` : float, default 0
        Value to replace NaNs in reflectivity before flow computation.
        (NaNs from missing data, clutter removal, etc.)

    - `flow_params` : dict, optional
        OpenCV Farneback optical flow parameters:

        - `pyr_scale` : float, default 0.5
            Image pyramid scale (0.5 = 2x reduction per level)

        - `levels` : int, default 3
            Number of pyramid levels

        - `winsize` : int, default 10
            Averaging window size

        - `iterations` : int, default 3
            Iterations at each pyramid level

        - `poly_n` : int, default 5
            Polynomial expansion size

        - `poly_sigma` : float, default 1.2
            Gaussian standard deviation

    - `global` : dict, optional
        - `var_names` : dict
            - `reflectivity` : str, reflectivity variable name (default: "reflectivity")

    Notes
    -----
    - Requires exactly 2 datasets (previous frame and current frame)
    - Flow is computed at previous-to-current transition
    - Registration (offset=0) uses flow from t-1→t0, projects t-1 labels
    - Future projections (offset=1+) extrapolate t0 forward using same flow
    - Fails gracefully: returns current dataset without projections if time gap too large
    - Processing time: 50-200 ms per frame pair
    - Projection results are cumulative: each step adds to displacement

    Examples
    --------
    >>> config = {
    ...     "method": "adapt_default",
    ...     "max_projection_steps": 3,
    ...     "max_time_interval_minutes": 30,
    ...     "flow_params": {"levels": 3, "winsize": 10}
    ... }
    >>> projector = RadarCellProjector(config)
    >>> ds_with_motion = projector.project([ds_t1, ds_t0])
    >>> num_projections = ds_with_motion["cell_projections"].shape[0]
    >>> print(
    ...     f"Generated {num_projections} projections (1 registration + {num_projections-1} future)"
    ... )
    """

    def __init__(self, config):
        """Initialize projector with validated configuration.

        Parameters
        ----------
        config : InternalConfig
            Fully validated runtime configuration.

        Notes
        -----
        All parameters are read directly from config - no defaults,
        no .get() calls, no validation. Configuration is already
        complete and validated by Pydantic.

        Examples
        --------
        >>> from adapt.configuration.schemas import resolve_config, ParamConfig
        >>> config = resolve_config(ParamConfig())
        >>> projector = RadarCellProjector(config)
        """
        self.method = config.method
        self.nan_fill = config.nan_fill_value
        self.max_interval_minutes = config.max_time_interval_minutes
        self.max_proj_steps = config.max_projection_steps
        self.flow_params = {
            "pyr_scale": config.pyr_scale,
            "levels": config.levels,
            "winsize": config.winsize,
            "iterations": config.iterations,
            "poly_n": config.poly_n,
            "poly_sigma": config.poly_sigma,
            "flags": config.flags,
        }
        self.min_motion_threshold = config.min_motion_threshold
        self.max_flow_magnitude = config.max_flow_magnitude
        self.registration_step_minutes = config.registration_step_minutes
        self.projection_horizon_minutes = config.projection_horizon_minutes
        self.refl_var = config.reflectivity_var

    def project(self, ds_list):
        """Project cells forward using optical flow motion vectors.

        Computes optical flow from reflectivity patterns between two consecutive
        frames, then applies the flow to project cell labels backward (registration)
        and forward (future predictions). Output includes:
        - Projected cell labels (one per projection step)
        - Motion vectors (heading_x, heading_y in pixels/frame)

        Parameters
        ----------
        ds_list : list of xr.Dataset
            Exactly two datasets: [ds_previous, ds_current]

            Each dataset must contain:
            - Dimensions: (y, x) - 2D (pre-sliced at fixed altitude)
            - Data variables: cell_labels, reflectivity
            - Coordinates: x, y
            - Attributes: time (datetime), z_level_m (altitude)

        Returns
        -------
        xr.Dataset
            Modified copy of ds_current with additional variables:

            - `cell_projections` : DataArray with dims (frame_offset, y, x)
                Projected cell labels for each time offset
                - frame_offset=0: Registration (t-1 cells projected to t0)
                - frame_offset=1+: Future projections (t0 cells extrapolated)
                - Values: cell label IDs (0=background, 1+=cells)

            - `heading_x`, `heading_y` : DataArray with dims (y, x)
                Optical flow displacement vectors in pixels/frame
                - Positive: rightward/downward motion
                - Negative: leftward/upward motion

            If time gap exceeds max_time_interval_minutes or other validation
            fails, returns ds_current WITHOUT projections (logged as warning).

        Notes
        -----
        - Requires time coordinates in both datasets for gap validation
        - Dispatches to implementation based on config["method"]
        - Currently only "adapt_default" (Farneback) is supported
        - Processing time: 50-200 ms per frame pair
        - Flow computation is robust to NaN values (filled with nan_fill_value)
        - Projection is deterministic: same input always produces same output

        Examples
        --------
        >>> projector = RadarCellProjector(config)
        >>> ds_with_motion = projector.project([ds_frame_t1, ds_frame_t0])
        >>>
        >>> # Access projections
        >>> registration = ds_with_motion["cell_projections"].sel(frame_offset=0)
        >>> future_1 = ds_with_motion["cell_projections"].sel(frame_offset=1)
        >>>
        >>> # Access motion vectors
        >>> vx = ds_with_motion["heading_x"]
        >>> vy = ds_with_motion["heading_y"]
        >>> speed = np.sqrt(vx**2 + vy**2)  # pixels/frame
        """
        return self._project_opticalflow(ds_list)

    def _project_opticalflow(self, ds_list):
        """Compute optical flow and project cell labels.

        Receives ds with cell_labels from segmenter. The reflectivity
        slice extraction is handled by processor before calling this.

        Note: Processor orchestration may validate time gaps earlier, but this
        method also enforces max_time_interval_minutes for standalone safety.
        """
        # Validate basic requirements (including time gap).
        time_diff = self._validate_datasets(ds_list, self.max_interval_minutes)
        if abs(time_diff) > self.max_interval_minutes:
            logger.warning(
                "Skipping projection: time interval %.1f min exceeds max %.1f min",
                float(time_diff),
                float(self.max_interval_minutes),
            )
            return ds_list[1].copy()

        logger.debug(f"Computing flow: {time_diff:.1f} min interval")

        # Get reflectivity from ds (already at correct z-level from processor)
        # Reflectivity is always 2D at the configured z-level
        refl1 = np.nan_to_num(ds_list[0][self.refl_var].values, nan=self.nan_fill).astype(
            np.float32
        )
        refl2 = np.nan_to_num(ds_list[1][self.refl_var].values, nan=self.nan_fill).astype(
            np.float32
        )

        refl1_norm, refl2_norm = self._normalize(refl1, refl2)
        flow = cv2.calcOpticalFlowFarneback(refl1_norm, refl2_norm, None, **self.flow_params)
        flow = self._sanitize_flow(flow)

        # Get cell_labels from segmenter output
        # For registration (offset=0): project labels from t-1 (ds_list[0]) to t0 (ds_list[1])
        # For future projections (offset=1, 2, 3, n): project labels from t0 (ds_list[1]) forward
        labels_prev = ds_list[0]["cell_labels"].values.astype(np.int32)
        labels_curr = ds_list[1]["cell_labels"].values.astype(np.int32)

        # Generate projections:
        # - First projection (offset=0) is registration: t-1 → t0 (uses labels from t-1)
        # - Subsequent projections (offset=1,2,...) are future: t0→t1, t1→t2, etc.
        #   (uses labels from t0)
        # So total projections = max_proj_steps + 1 (1 for registration + N for future)

        labels_proj_list = []

        # Registration - project t-1 labels to t0 position (1 step)
        registration = self._project_frames(labels_prev, flow, n_steps=1)
        labels_proj_list.append(registration[0])

        # Future projections - project current labels (t0) forward (n steps)
        # Each pixel carries its original flow value and uses accumulated displacement.
        # @TODO I have removed more complecated logic of using flow at new positions for each step,
        # because some cells did not move in noisy radar data during the test.
        future_projections = self._project_frames(labels_curr, flow, n_steps=self.max_proj_steps)
        for i in range(self.max_proj_steps):
            labels_proj_list.append(future_projections[i])

        # Add cell_projections to the second (latest) ds
        ds_out = ds_list[1].copy()

        # Stack projections along frame_offset dimension
        # frame_offset=0: registration (projection from t-1 to t0)
        # frame_offset=1,2,...: future projections from t0
        if labels_proj_list:
            frame_offsets = list(range(len(labels_proj_list)))  # 0, 1, 2, ... (0=registration)
            projections = np.stack(labels_proj_list, axis=0)

            ds_out["cell_projections"] = xr.DataArray(
                projections,
                dims=["frame_offset", "y", "x"],
                coords={
                    "frame_offset": frame_offsets,
                    "y": ds_out.y,
                    "x": ds_out.x,
                },
                attrs={"description": "Projected cell labels"},
            )

            # Also store flow field
            ds_out["heading_x"] = xr.DataArray(
                flow[:, :, 0].astype(np.float32),
                dims=["y", "x"],
                coords={"y": ds_out.y, "x": ds_out.x},
                attrs={
                    "units": "pixels/frame",
                    "description": "Heading in x direction",
                },
            )
            ds_out["heading_y"] = xr.DataArray(
                flow[:, :, 1].astype(np.float32),
                dims=["y", "x"],
                coords={"y": ds_out.y, "x": ds_out.x},
                attrs={
                    "units": "pixels/frame",
                    "description": "heading in y direction",
                },
            )

            logger.info(f"Added cell_projections with {len(labels_proj_list)} projection steps")

        # Minute-resolution registration: previous labels advected at each whole
        # minute in (t_prev, t_curr] using the same flow, fractionally scaled.
        # This is the geometry product downstream association modules consume.
        t_prev = self._frame_time(ds_list[0])
        t_curr = self._frame_time(ds_list[1])
        minutes = self._registration_minute_times(t_prev, t_curr)
        fractions = ((minutes - t_prev) / (t_curr - t_prev)).to_numpy().astype(np.float32)
        minute_frames = self._project_fractions(labels_prev, flow, list(fractions))

        ds_out["registration_minutes"] = xr.DataArray(
            minute_frames,
            dims=["minute", "y", "x"],
            coords={"minute": minutes.values, "y": ds_out.y, "x": ds_out.x},
            attrs={
                "description": (
                    "Previous scan's cell labels advected to each whole minute "
                    "between the scan pair (label space: previous scan)"
                )
            },
        )
        ds_out = ds_out.assign_coords(interpolation_fraction=("minute", fractions))
        ds_out["interpolation_fraction"].attrs["description"] = (
            "Minutes elapsed since the previous scan divided by the scan-pair gap"
        )

        # Minute-resolution forward projection: current labels advected to each
        # whole minute in (t_curr, t_curr + horizon]. Same flow, same fill;
        # forward fractions may exceed 1 when the horizon reaches beyond one
        # scan gap (Lagrangian extrapolation).
        proj_minutes = self._projection_minute_times(t_curr, self.projection_horizon_minutes)
        proj_fractions = ((proj_minutes - t_curr) / (t_curr - t_prev)).to_numpy().astype(np.float32)
        proj_frames = self._project_fractions(labels_curr, flow, list(proj_fractions))

        ds_out["projection_minutes"] = xr.DataArray(
            proj_frames,
            dims=["projection_minute", "y", "x"],
            coords={"projection_minute": proj_minutes.values, "y": ds_out.y, "x": ds_out.x},
            attrs={
                "description": (
                    "Current scan's cell labels advected forward to each whole minute "
                    "after the scan (label space: current scan)"
                )
            },
        )
        ds_out = ds_out.assign_coords(projection_fraction=("projection_minute", proj_fractions))
        ds_out["projection_fraction"].attrs["description"] = (
            "Minutes after the current scan divided by the scan-pair gap (may exceed 1)"
        )

        # Store projection metadata for contract validation
        # This enables self-describing datasets: validators can read runtime config
        # from dataset attributes without needing context access
        ds_out.attrs.update(
            {
                "max_projection_steps": self.max_proj_steps,
                "num_projection_steps": (len(labels_proj_list) if labels_proj_list else 0),
                "projection_method": "adapt_default",
                "projection_horizon_minutes": self.projection_horizon_minutes,
                "registration_source_scan_time": pd.Timestamp(t_prev).isoformat(),
                "registration_target_scan_time": pd.Timestamp(t_curr).isoformat(),
            }
        )

        return ds_out

    @staticmethod
    def _frame_time(ds: xr.Dataset) -> pd.Timestamp:
        """Return a frame's scan time, whatever shape the time coord arrives in.

        Production grids carry time as a plain scalar, a length-1 array, a 0-d
        ndarray (object or non-nanosecond datetime64), or a cftime calendar
        object (xarray's decoding of pyart grid NetCDFs). pd.Timestamp rejects
        0-d ndarrays and cftime directly, so normalize before converting.
        """
        value = ds.time.values
        if np.ndim(value) > 0:
            value = value[0]
        if isinstance(value, np.ndarray):  # 0-d ndarray: unwrap to its scalar
            value = value.item()
        if not isinstance(value, (str, datetime, np.datetime64)) and hasattr(
            value, "isoformat"
        ):  # cftime calendar datetimes are not datetime subclasses
            value = value.isoformat()
        return pd.Timestamp(value)

    def _registration_minute_times(
        self, t_prev: pd.Timestamp, t_curr: pd.Timestamp
    ) -> pd.DatetimeIndex:
        """Whole-minute grid points m with t_prev < m <= t_curr.

        The grid is epoch-aligned (multiples of the step since epoch), so the
        minute timestamps stay continuous across consecutive scan pairs instead
        of drifting with each scan's start time.
        """
        step = pd.Timedelta(minutes=self.registration_step_minutes)
        first = t_prev.floor(f"{self.registration_step_minutes}min") + step
        return pd.date_range(first, t_curr, freq=step)

    def _projection_minute_times(
        self, t_curr: pd.Timestamp, horizon_minutes: int
    ) -> pd.DatetimeIndex:
        """Whole-minute grid points m with t_curr < m <= t_curr + horizon_minutes.

        Shares the epoch-aligned grid step with registration so the backward and
        forward minute timelines stay continuous across t_prev -> t_curr -> horizon.
        """
        step = pd.Timedelta(minutes=self.registration_step_minutes)
        first = t_curr.floor(f"{self.registration_step_minutes}min") + step
        last = t_curr + pd.Timedelta(minutes=horizon_minutes)
        return pd.date_range(first, last, freq=step)

    def _project_frames(self, labels_src, flow, n_steps=1):
        """Project labels for whole flow steps (1, 2, ..., n_steps)."""
        return self._project_fractions(labels_src, flow, [float(s) for s in range(1, n_steps + 1)])

    def _project_fractions(self, labels_src, flow, fractions):
        """Project labels by fractional multiples of the flow field.

        Key concept: Flow is computed at original pixel positions (t-1→t0).
        Each pixel carries its original flow value and uses it for every
        projected frame; frame i displaces by ``flow * fractions[i]``.
        A whole step is fraction 1.0; minute interpolation uses 0 < f < 1.

        Args:
            labels_src: Source labels (H, W) at time t
            flow: Optical flow field (H, W, 2) in pixels/frame
            fractions: Flow multiples, one output frame per entry

        Returns:
            projections: (len(fractions), H, W) array with projected labels
        """
        H, W = labels_src.shape
        n_steps = len(fractions)
        projections = np.full((n_steps, H, W), fill_value=0, dtype=np.int32)
        if n_steps == 0:
            return projections

        unique_labels = np.unique(labels_src[labels_src > 0])

        # Sort by area (smallest first) to prevent large cells from overwriting small ones
        label_areas = []
        for label_val in unique_labels:
            area = np.sum(labels_src == label_val)
            label_areas.append((label_val, area))
        label_areas.sort(key=lambda x: x[1])

        # For each cell, extract pixels with their flow values ONCE
        for label_val, _ in label_areas:
            mask = labels_src == label_val
            y_indices, x_indices = np.where(mask)

            # Extract flow at ORIGINAL positions - this travels with the pixel
            cell_pixels = []
            for idx in range(len(y_indices)):
                y = y_indices[idx]
                x = x_indices[idx]

                # Get flow at this pixel's ORIGINAL location
                fx = flow[y, x, 0]
                fy = flow[y, x, 1]

                # If flow is invalid, use zero (pixel doesn't move)
                if not np.isfinite(fx) or not np.isfinite(fy):
                    fx, fy = 0.0, 0.0

                cell_pixels.append((y, x, fx, fy))

            # Project this cell for all frames using SAME flow values
            for step_idx in range(n_steps):
                fraction = fractions[step_idx]

                for y, x, fx, fy in cell_pixels:
                    # Accumulated displacement: original_pos + flow * fraction
                    new_x = x + fx * fraction
                    new_y = y + fy * fraction

                    new_x_int = int(np.round(new_x))
                    new_y_int = int(np.round(new_y))

                    # Only place if within bounds
                    if 0 <= new_x_int < W and 0 <= new_y_int < H:
                        projections[step_idx, new_y_int, new_x_int] = label_val

        # Fill holes in projected cells using concave hull for each step
        for step_idx in range(n_steps):
            for label_val in unique_labels:
                label_mask = projections[step_idx] == label_val

                if not label_mask.any():
                    continue

                filled_mask = self._fill_concave_hull(label_mask, alpha=0.1)
                projections[step_idx][filled_mask > 0] = label_val

        return projections

    def _validate_datasets(self, ds_list, max_interval_minutes):
        """Validate dataset appropriateness.

        Note: Time gap validation is now handled by Processor. This method
        just computes the time difference for logging purposes.
        """
        if len(ds_list) != 2:
            raise ValueError(f"Need exactly 2 datasets, got {len(ds_list)}")

        # Handle both scalar and array time coordinates (2D datasets have scalar time)
        time1_val = ds_list[0].time.values
        time2_val = ds_list[1].time.values
        time1 = time1_val if np.ndim(time1_val) == 0 else time1_val[0]
        time2 = time2_val if np.ndim(time2_val) == 0 else time2_val[0]

        time_diff_minutes = (time2 - time1) / np.timedelta64(1, "m")

        # Note: Processor already validated time gap, so we just warn if large
        if abs(time_diff_minutes) > max_interval_minutes:
            logger.warning(
                f"Time interval {time_diff_minutes:.1f} min exceeds max "
                f"{max_interval_minutes} min. Processor should have filtered this pair."
            )

        return time_diff_minutes

    def _sanitize_flow(self, flow: np.ndarray) -> np.ndarray:
        """Apply sanity checks to Farneback flow field.

        Clips vectors exceeding max_flow_magnitude (spurious large displacements
        at data edges or in no-echo regions). Logs a warning when median motion
        falls below min_motion_threshold (field likely static or noisy).

        Parameters
        ----------
        flow : np.ndarray, shape (H, W, 2)
            Raw optical flow from cv2.calcOpticalFlowFarneback.

        Returns
        -------
        np.ndarray
            Flow with outlier vectors clipped; same shape and dtype.
        """
        magnitude = np.linalg.norm(flow, axis=2)

        median_mag = float(np.median(magnitude))
        if median_mag < self.min_motion_threshold:
            logger.warning(
                "Median flow magnitude %.2f px/frame is below min_motion_threshold %.2f; "
                "field may be static or dominated by noise",
                median_mag,
                self.min_motion_threshold,
            )

        too_large = magnitude > self.max_flow_magnitude
        if too_large.any():
            n_clipped = int(too_large.sum())
            logger.warning(
                "Clipping %d flow vectors exceeding max_flow_magnitude %.1f px/frame "
                "(max observed: %.1f)",
                n_clipped,
                self.max_flow_magnitude,
                float(magnitude.max()),
            )
            # Scale down vectors that exceed the cap, preserving direction.
            scale = np.where(too_large, self.max_flow_magnitude / np.maximum(magnitude, 1e-6), 1.0)
            flow = flow * scale[:, :, np.newaxis]

        return flow

    def _normalize(self, refl1, refl2):
        """Normalize to uint8."""
        vmin = min(refl1.min(), refl2.min())
        vmax = max(refl1.max(), refl2.max())

        if vmax > vmin:
            refl1_norm = np.uint8(255 * (refl1 - vmin) / (vmax - vmin))
            refl2_norm = np.uint8(255 * (refl2 - vmin) / (vmax - vmin))
        else:
            refl1_norm = np.uint8(refl1)
            refl2_norm = np.uint8(refl2)

        return refl1_norm, refl2_norm

    def _fill_concave_hull(self, label_mask, alpha=0.1):
        """Fill concave hull using alpha shapes.

        Args:
            label_mask: Binary mask of projected points
            alpha: Controls tightness (lower = tighter, higher = more convex)
                   Typical range: 0.05-0.3
        """
        if not label_mask.any():
            return label_mask

        # Get coordinates of projected points
        points = np.argwhere(label_mask)

        if len(points) < 4:
            # Too few points for triangulation, use dilation
            kernel = np.ones((3, 3), dtype=np.uint8)
            return binary_dilation(label_mask, structure=kernel).astype(np.uint8)

        # Swap to (x, y) for Delaunay
        points = points[:, [1, 0]]

        # A collinear / single-row / single-column point set is < 2-D, so Delaunay
        # would raise QhullError ("input is less than 3-dimensional"). This is an
        # expected case for thin cells, not an error: detect it up front and fall back
        # to dilation silently instead of logging a verbose qhull dump per cell.
        if np.linalg.matrix_rank(points - points.mean(axis=0)) < 2:
            kernel = np.ones((3, 3), dtype=np.uint8)
            return binary_dilation(label_mask, structure=kernel).astype(np.uint8)

        try:
            # Compute Delaunay triangulation
            tri = Delaunay(points)

            # Create output mask
            filled = np.zeros_like(label_mask, dtype=np.uint8)

            # Filter triangles by circumradius (alpha shape)
            for simplex in tri.simplices:
                # Get triangle vertices
                pts = points[simplex]

                # Compute circumradius
                a = np.linalg.norm(pts[1] - pts[0])
                b = np.linalg.norm(pts[2] - pts[1])
                c = np.linalg.norm(pts[0] - pts[2])

                # Semi-perimeter
                s = (a + b + c) / 2.0
                area = np.sqrt(max(0, s * (s - a) * (s - b) * (s - c)))

                if area > 1e-10:
                    circumradius = (a * b * c) / (4.0 * area)

                    # Keep triangle if circumradius < 1/alpha
                    if circumradius < 1.0 / alpha:
                        # Fill triangle using cv2
                        triangle = pts.astype(np.int32).reshape((-1, 1, 2))
                        cv2.fillConvexPoly(filled, triangle, 1)

            return filled.astype(np.uint8)

        except Exception as e:
            # Unexpected failure (degenerate input is handled above). Keep it to one
            # concise line — never dump the full multi-line qhull diagnostic.
            logger.warning("Concave hull fill failed (%s); using dilation", type(e).__name__)
            kernel = np.ones((3, 3), dtype=np.uint8)
            return binary_dilation(label_mask, structure=kernel).astype(np.uint8)
