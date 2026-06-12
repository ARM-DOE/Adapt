# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Project lightning lon/lat onto ADAPT's projected grid.

ADAPT grids are azimuthal-equidistant in metres centred on the radar
(``+proj=aeqd +lat_0=<radar_lat> +lon_0=<radar_lon> +units=m``), so flash
coordinates must use the same projection to align with the cell-mask grid.
"""

import numpy as np
from pyproj import Transformer


def project_lonlat_to_xy(
    lon: np.ndarray, lat: np.ndarray, radar_lat: float, radar_lon: float
) -> tuple[np.ndarray, np.ndarray]:
    """Transform lon/lat (degrees) to grid x/y (metres) for the given radar origin."""
    transformer = Transformer.from_crs(
        "EPSG:4326",
        f"+proj=aeqd +lat_0={radar_lat} +lon_0={radar_lon} +x_0=0 +y_0=0 +datum=WGS84 +units=m",
        always_xy=True,
    )
    x, y = transformer.transform(np.asarray(lon), np.asarray(lat))
    return np.asarray(x), np.asarray(y)
