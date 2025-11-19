"""
Collection of various helper functions.

Note: Plotting utilities are located in a separate module.
"""
import io
import logging
from contextlib import contextmanager, redirect_stdout
from functools import lru_cache
from typing import Tuple

import backoff
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim
from pyproj import Transformer

from worldpoppy.config import WGS84_CRS

__all__ = [
    "geolocate_name",
    "module_available",
    "log_info_context"
]


@lru_cache(maxsize=1024)
@backoff.on_exception(
    backoff.expo, GeocoderTimedOut, max_tries=5, jitter=backoff.full_jitter
)
def geolocate_name(nomatim_query, to_crs=None):
    """
    Return the geo-coordinate associated with a given location name,
    based on search results from OSM's 'Nominatim' service.

    Parameters
    ----------
    nomatim_query : str
        A location name to be geocoded.
    to_crs : pyproj.CRS or str, optional
        If specified, transforms the returned coordinate from (lon, lat)
        to this CRS.

    Returns
    -------
    Tuple[float, float]
        The (x, y) coordinate in the target CRS, or (lon, lat) in WGS84
        if `to_crs` is None.

    Raises
    ------
    RuntimeError
        If the Nominatim query has returned None.
    """
    geolocator = Nominatim(user_agent="MyLocationCacher", timeout=2)
    located = geolocator.geocode(nomatim_query)

    if located is None:
        raise RuntimeError(f"Nomatim search for location name '{nomatim_query}' returned no hit.")

    lon, lat = located.point.longitude, located.point.latitude
    if to_crs is None:
        return lon, lat

    transformer = Transformer.from_crs(WGS84_CRS, to_crs, always_xy=True)
    x, y = transformer.transform(lon, lat)
    return x, y


def module_available(module_name):
    """Check if a named Python module is available for import."""
    try:
        exec(f"import {module_name}")
    except ModuleNotFoundError:
        return False
    else:
        return True


@contextmanager
def log_info_context(logger):
    """
    Context manager to optionally redirect `print` statements to a logger.

    If the logger's effective level is WARNING or higher (default),
    `print()` statements execute normally. On lower logging levels,
    `print()` outputs are captured and sent to logger.info() instead.

    Parameters
    ----------
    logger : logging.Logger
        The logger instance to use (e.g., from `logging.getLogger(__name__)`).
    """
    effective_level = logger.getEffectiveLevel()

    if effective_level <= logging.INFO:
        string_buffer = io.StringIO()

        try:
            # use the thread-safe stdout redirector
            with redirect_stdout(string_buffer):
                yield  # user's `print()` runs here
        finally:
            # after the block, get the captured text
            captured_message = string_buffer.getvalue().strip()
            if captured_message:
                # log the captured text instead of printing
                logger.info(captured_message)

    else:
        # logger is not set to INFO, so we don't interfere
        try:
            yield
        finally:
            pass  # nothing to clean up
