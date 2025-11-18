import logging

import pytest

from tests.test_utils import needs_internet, isolated_raster_cache, no_manifest_update


@pytest.mark.e2e
@needs_internet
def test_e2e_dry_run_respects_cache(isolated_raster_cache, no_manifest_update, caplog):
    """
    Runs a full e2e test of the dry_run logic, ensuring it
    correctly respects the state of the local cache.
    """
    from worldpoppy import wp_raster

    product = 'ppp'
    aoi = 'LIE'  # Liechtenstein, a very small country
    years = [2010, 2011, 2012]

    # define one of the expected paths in our isolated cache
    expected_eg_fpath = isolated_raster_cache / "ppp_LIE_2010.tif"

    # --- 1. First call (Cold Cache) ---
    # cache is empty, so this MUST report files to download
    caplog.clear()
    caplog.set_level(logging.INFO)  # needed to trigger propper logging in `download`

    result = wp_raster(
        product_name=product,
        aoi=aoi,
        years=years,
        download_dry_run=True
    )

    assert result is None  # dry_run should return None
    assert not expected_eg_fpath.is_file()  # no file downloaded

    # check the logged output
    assert "No. of files to download: 3" in caplog.text
    # ensure it is reporting a real size
    assert "Total est. download size: 0.0 MB" not in caplog.text

    # --- 2. Second call (Real Download) ---
    # this should download the file and populate the cache
    result = wp_raster(
        product_name=product,
        aoi=aoi,
        years=years,
        download_dry_run=False
    )

    assert result is not None  # real run returns an xarray
    assert expected_eg_fpath.is_file()  # file MUST now exist
    assert expected_eg_fpath.stat().st_size > 0

    # --- 3. Third call (Warm Cache) ---
    # cache is full, so this MUST report 0 files to download
    caplog.clear()

    result = wp_raster(
        product_name=product,
        aoi=aoi,
        years=years,
        download_dry_run=True
    )

    assert result is None

    # check the logged output
    assert "No. of files to download: 0" in caplog.text
    assert "Total est. download size: 0.0 MB" in caplog.text
