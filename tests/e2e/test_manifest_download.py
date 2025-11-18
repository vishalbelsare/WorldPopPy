import logging
import time
import pytest
from tests.test_utils import needs_internet, isolated_manifest_assets


@pytest.mark.e2e
@needs_internet
def test_e2e_manifest_download_with_timestamp_logic(isolated_manifest_assets, caplog):
    """
    Runs a full e2e test of the manifest download and caching logic,
    including the logic for touching the '_last_check_date_fpath' file.
    """
    from worldpoppy.manifest import build_wp_manifest

    # set logger to see the "Downloading" and "Cleaned" warnings
    caplog.set_level(logging.WARNING, logger="worldpoppy.manifest")

    # --- 1. First call (Cold Start) ---
    # cache is empty, so this MUST download.
    build_wp_manifest()

    # check that the expected files were created in the temp dir
    temp_hash_path = isolated_manifest_assets / "raw_manifest_hash.txt"
    temp_check_date_path = isolated_manifest_assets / "last_manifest_check.txt"
    temp_manifest_path = isolated_manifest_assets / "manifest.feather"

    assert temp_hash_path.is_file()
    assert temp_hash_path.stat().st_size > 0
    assert temp_manifest_path.is_file()
    assert temp_manifest_path.stat().st_size > 0
    assert temp_check_date_path.is_file()
    assert temp_check_date_path.stat().st_size == 0  # touched only
    touch_time1 = temp_check_date_path.stat().st_mtime

    # check that the log also shows a download happened
    assert "Downloading fresh WorldPop data manifest" in caplog.text
    assert "Cleaned WorldPop data manifest has been stored" in caplog.text

    # --- 2. Second call (Warm Cache) ---
    # nothing should happen since cache is populated and up-to-date
    caplog.clear()  # clear logs from the first run
    time.sleep(0.1)  # ensure touch times will be different
    build_wp_manifest()

    # check that NO new download happened
    assert "Downloading fresh WorldPop data manifest" not in caplog.text
    assert "Cleaned WorldPop data manifest has been stored" not in caplog.text

    # check that the check timestamp was still updated
    touch_time2 = temp_check_date_path.stat().st_mtime
    assert touch_time2 > touch_time1

    # --- 3. Third call (Forced Download) ---
    # forcing with overwrite=True should download again
    caplog.clear()
    time.sleep(0.1)
    build_wp_manifest(overwrite=True)

    # check that a download happened again
    assert "Downloading fresh WorldPop data manifest" in caplog.text
    assert "Cleaned WorldPop data manifest has been stored" in caplog.text

    # check that the check timestamp was again updated
    touch_time3 = temp_check_date_path.stat().st_mtime
    assert touch_time3 > touch_time2
