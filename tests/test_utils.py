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


def is_online():
    """Check if we can connect to a known external server."""
    try:
        # 8.8.8.8 is Google's DNS. 53 is the DNS port.
        socket.create_connection(("8.8.8.8", 53), timeout=1)
        return True
    except OSError:
        return False

# create a custom "mark" that skips if we are
needs_internet = pytest.mark.skipif(not is_online(), reason="No internet connection")
