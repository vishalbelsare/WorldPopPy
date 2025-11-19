import ftplib
import logging
from unittest.mock import patch, MagicMock

import numpy as np
import pytest

from tests.test_utils import no_manifest_update


def test_good_year_extraction():
    from worldpoppy.manifest import extract_year

    year = extract_year('some_dataset_2020')
    assert year == 2020


def test_bad_year_extraction_raises():
    from worldpoppy.manifest import extract_year

    with pytest.raises(ValueError):
        extract_year('bad_name')

    with pytest.raises(ValueError):
        extract_year('bad_name_1889')
        extract_year('bad_name_1999')  # first supported year of any dataset is 2000


def test_year_stripping_for_product_name():
    from worldpoppy.manifest import _strip_year_from_product_name
    assert _strip_year_from_product_name('some_dataset_2020') == 'some_dataset'
    assert _strip_year_from_product_name('some_dataset_2020_constrained') == 'some_dataset_constrained'


def test_year_stripping_for_note():
    from worldpoppy.manifest import _strip_years_from_note
    assert _strip_years_from_note('total people in 2020') == 'total people'
    assert _strip_years_from_note('area edges 2000') == 'area edges'

    # we don't remove 4-digit expressions that would imply years before 2000
    assert _strip_years_from_note('total people in 1999') == 'total people in 1999'
    assert _strip_years_from_note('area edges 1889') == 'area edges 1889'

def test_looks_like_annual_name():
    from worldpoppy.manifest import _looks_like_annual_name

    assert _looks_like_annual_name('foo_2020') is True
    assert _looks_like_annual_name('foo_2020_to_2020') is False
    assert _looks_like_annual_name('foo') is False


def test_manifest_filter_by_all_years_drops_static(no_manifest_update):
    from worldpoppy.manifest import wp_manifest

    mdf = wp_manifest()  # full manifest
    expected = mdf[mdf.is_annual]
    actual = wp_manifest(years='all')
    assert np.all(expected.idx == actual.idx)


def test_manifest_filter_annual_product_success(no_manifest_update):
    from worldpoppy.manifest import wp_manifest

    def _check_result():
        assert np.all(mdf.product_name == product_name)
        assert np.all(mdf.iso3.isin(iso3_codes))
        assert np.all(mdf.year.isin(years))

    # example 1
    iso3_codes = ['COD', 'CAF', 'SSD', 'SDN']
    product_name = 'ppp'
    years = [2018, 2019, 2020]
    mdf = wp_manifest(product_name, iso3_codes, years=years)
    _check_result()

    # example 2
    iso3_codes = ['DNK', 'NOR', 'SWE', 'FIN']
    product_name = 'agesex_f_60_constrained_UNadj'
    years = [2020]
    mdf = wp_manifest(product_name, iso3_codes, years=years)
    _check_result()


def test_manifest_filter_static_product_success(no_manifest_update):
    from worldpoppy.manifest import wp_manifest

    def _check_result():
        assert np.all(mdf.product_name == product_name)
        assert np.all(mdf.iso3.isin(iso3_codes))
        assert np.all(np.isnan(mdf.year))

    # example 1
    iso3_codes = ['USA', 'CAN', 'MEX']
    product_name = 'srtm_slope_100m'
    mdf = wp_manifest(product_name, iso3_codes, years=None)
    _check_result()

    # example 2
    iso3_codes = ['MYS', 'SGP', 'IDN']
    product_name = 'dst_coastline_100m_2000_2020'
    mdf = wp_manifest(product_name, iso3_codes, years=None)
    _check_result()


def test_manifest_filter_invalid_inputs_raise(no_manifest_update):
    from worldpoppy.manifest import wp_manifest

    with pytest.raises(ValueError):
        wp_manifest(product_name='no_real_product')

    with pytest.raises(ValueError):
        wp_manifest(iso3_codes='fantasia')

    with pytest.raises(ValueError):
        wp_manifest(years=1900)

    with pytest.raises(ValueError, match="but not both"):
        wp_manifest(product_name='ppp', keyword='pop')


def test_manifest_constrained_unavailable_combo_raises(no_manifest_update):
    from worldpoppy.manifest import wp_manifest, wp_manifest_constrained
    eg_prod, eg_iso, eg_year = 'viirs_100m', 'NZL' , 2020

    wp_manifest(product_name=eg_prod)
    wp_manifest(iso3_codes=eg_iso)
    wp_manifest(years=eg_year)

    with pytest.raises(ValueError):
        # empty combo (incomplete coverage)
        wp_manifest_constrained(product_name=eg_prod, iso3_codes=eg_iso, years=eg_year)


def test_worldpop_ftp_download_retries_on_temp_error(caplog):
    """
    Unit test to check whether the @backoff decorator retries
    on transient FTP errors (ftplib.error_temp, i.e., 4xx).
    """

    caplog.set_level(logging.DEBUG, logger="backoff")

    # create the temporary error (a 4xx FTP reply)
    temp_error = ftplib.error_temp("421 Service not available (Mocked Error)")

    # create a "success" object
    mock_success_client = MagicMock()
    mock_success_client.retrbinary.return_value = "226 Transfer complete."

    # patch 'ftplib.FTP' *where it is used* (in the manifest module)
    with patch("worldpoppy.manifest.ftplib.FTP") as mock_ftp_constructor:

        # configure the side_effect to fail twice, then succeed
        mock_ftp_constructor.side_effect = [
            temp_error,
            temp_error,
            mock_success_client
        ]

        # call the tested function (downloading to memory)
        from worldpoppy.manifest import _worldpop_ftp_download
        _worldpop_ftp_download('/assets/wpgpDatasets.md5', local_fpath=None)

        # check it was called 3 times
        assert mock_ftp_constructor.call_count == 3

        # check that successful client was the one used for the download
        mock_success_client.retrbinary.assert_called_once()

        # check that the backoff logger actually logged its retry attempts
        assert "Backing off" in caplog.text
        assert caplog.text.count("Backing off") == 2
