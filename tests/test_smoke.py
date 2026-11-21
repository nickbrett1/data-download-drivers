"""Smoke test: the src-layout package installs and imports cleanly."""


def test_package_imports():
    import data_download_drivers

    assert data_download_drivers.__version__
