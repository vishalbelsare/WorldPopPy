import logging

import pytest
from tests.test_utils import needs_internet


@pytest.fixture
def isolated_manifest_assets(monkeypatch, tmp_path):
    """
    Fixture to isolate manifest assets for an e2e download test.

    This patches the manifest module's internal path variables
    (_raw_hash_fpath, _cleaned_manifest_fpath), which normally
    point to ASSET_DIR. Instead, it redirects them to a new,
    empty temporary directory.

    This ensures the e2e test runs with a "cold start" (no existing
    manifest) and does not interfere with the user's real (or bundled)
    manifest files.

    Note: This fixture does *not* affect the raster cache directory.
    """
    from worldpoppy import manifest

    new_hash_path = tmp_path / "raw_manifest_hash.txt"
    new_manifest_path = tmp_path / "manifest.feather"

    # patch the module-level variables inside manifest.py
    monkeypatch.setattr(manifest, "_raw_hash_fpath", new_hash_path)
    monkeypatch.setattr(manifest, "_cleaned_manifest_fpath", new_manifest_path)

    # yield the temp directory path so the test can inspect it
    yield tmp_path


@pytest.mark.e2e
@needs_internet
def test_e2e_fresh_manifest_download(isolated_manifest_assets, caplog):
    """
    Runs a full e2e test of the manifest download and caching logic
    using a temporary, isolated asset directory.
    """
    from worldpoppy.manifest import build_wp_manifest

    # set logger to see the "Downloading" and "Cleaned" warnings
    caplog.set_level(logging.WARNING, logger="worldpoppy.manifest")

    # --- 1. First call (Cold Start) ---
    # Cache is empty, so this MUST download.
    build_wp_manifest()

    # check that the expected files were created in the temp dir
    temp_hash_path = isolated_manifest_assets / "raw_manifest_hash.txt"
    temp_manifest_path = isolated_manifest_assets / "manifest.feather"

    assert temp_hash_path.is_file()
    assert temp_hash_path.stat().st_size > 0
    assert temp_manifest_path.is_file()
    assert temp_manifest_path.stat().st_size > 0

    # check that the log also shows a download happened
    assert "Downloading fresh WorldPop data manifest" in caplog.text
    assert "Cleaned WorldPop data manifest has been stored" in caplog.text

    # --- 2. Second call (Warm Cache) ---
    # nothing should happen since cache is populated and up-to-date
    caplog.clear()  # clear logs from the first run
    build_wp_manifest()

    # check that NO new download happened
    assert "Downloading fresh WorldPop data manifest" not in caplog.text
    assert "Cleaned WorldPop data manifest has been stored" not in caplog.text

    # --- 3. Third call (Forced Download) ---
    # forcing with overwrite=True should download again
    caplog.clear()
    build_wp_manifest(overwrite=True)

    # check that a download happened again
    assert "Downloading fresh WorldPop data manifest" in caplog.text
    assert "Cleaned WorldPop data manifest has been stored" in caplog.text
