import pytest
import socket


@pytest.fixture
def no_manifest_update(monkeypatch):
    """
    Fixture to patch `build_wp_manifest` with a no-op function.

    This patch ensures that regular unit tests never trigger a manifest
    update check on the WorldPop FTP server. Tests will instead run
    against the locally cached manifest.
    """
    from worldpoppy import manifest

    monkeypatch.setattr(
        manifest,
        "build_wp_manifest",
        lambda *args, **kwargs: None,  # do nothing
    )


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
    new_check_date_path = tmp_path / "last_manifest_check.txt"

    # patch the module-level variables inside manifest.py
    monkeypatch.setattr(manifest, "_raw_hash_fpath", new_hash_path)
    monkeypatch.setattr(manifest, "_cleaned_manifest_fpath", new_manifest_path)
    monkeypatch.setattr(manifest, "_last_check_date_fpath", new_check_date_path)

    # yield the temp directory path so the test can inspect it
    yield tmp_path


@pytest.fixture
def isolated_raster_cache(monkeypatch, tmp_path):
    """
    Fixture to isolate the WorldPopPy raster cache for an e2e test.

    This patches the 'WORLDPOPPY_CACHE_DIR' environment variable to
    point to a new, empty temporary directory.

    This ensures the e2e test runs with a "cold start" (no existing
    raster cache) and does not interfere with the user's real cache.
    """

    new_cache_dir = tmp_path / "test_raster_cache"
    new_cache_dir.mkdir()
    monkeypatch.setenv(  # automatically handles teardown
        'WORLDPOPPY_CACHE_DIR', str(new_cache_dir)
    )

    # yield the temp cache path so the test can inspect it
    yield new_cache_dir


def is_online():
    """Check if we can connect to a known external server."""
    try:
        # 8.8.8.8 is Google's DNS. 53 is the DNS port.
        socket.create_connection(("8.8.8.8", 53), timeout=1)
        return True
    except OSError:
        return False

# create a custom "mark" that skips if we are offline
needs_internet = pytest.mark.skipif(not is_online(), reason="No internet connection")
