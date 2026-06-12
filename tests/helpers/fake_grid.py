import numpy as np
import xarray as xr


def make_fake_grid_ds(
    time_len=1,
    z_levels=(0, 1000, 2000),
    shape=(5, 5),
    variables=("reflectivity",),
    with_labels=True,
    seed=0,
):
    """
    Create a minimal Py-ART-like grid dataset suitable
    for processor integration tests.

    Random values are drawn from a seeded generator so every test run
    sees the identical field (determinism is a tested invariant).
    """
    rng = np.random.default_rng(seed)

    time = np.array(["2025-01-01"], dtype="datetime64[ns]")
    z = np.array(z_levels)
    y = np.arange(shape[0])
    x = np.arange(shape[1])

    data_vars = {}
    for var in variables:
        data = rng.random((time_len, len(z), shape[0], shape[1])) * 50
        data_vars[var] = (("time", "z", "y", "x"), data)

    ds = xr.Dataset(
        data_vars=data_vars,
        coords={"time": time, "z": z, "y": y, "x": x},
        attrs={
            "radar_latitude": 40.0,
            "radar_longitude": -100.0,
            "radar_altitude": 100.0,
        },
    )

    return ds


def make_textured_2d_ds(shape=(24, 24), seed=0, base=20.0, span=25.0):
    """Seeded 2D reflectivity field with spatial texture.

    Texture (per-pixel variation) gives optical flow and watershed real
    gradients to work on, while the fixed seed keeps the field identical
    across runs and processes.
    """
    rng = np.random.default_rng(seed)
    data = (base + rng.random(shape) * span).astype(np.float32)

    return xr.Dataset(
        {"reflectivity": (("y", "x"), data)},
        coords={"y": np.arange(shape[0]), "x": np.arange(shape[1])},
        attrs={"z_level_m": 2000},
    )


def make_fake_grid_ds_with_labels():
    data = {"reflectivity": (("y", "x"), np.ones((4, 4)))}

    data["cell_labels"] = (
        ("y", "x"),
        np.array(
            [
                [0, 1, 1, 0],
                [0, 1, 1, 0],
                [0, 0, 2, 2],
                [0, 0, 2, 2],
            ]
        ),
    )

    ds = xr.Dataset(
        data,
        coords={
            "x": np.arange(4),
            "y": np.arange(4),
        },
        attrs={"z_level_m": 2000},
    )
    return ds
