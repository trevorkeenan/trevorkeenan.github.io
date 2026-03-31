import unittest
from unittest import mock

from scripts import refresh_japanflux_direct as module


class RefreshJapanFluxDirectTests(unittest.TestCase):
    def setUp(self):
        module.DIRECT_DOWNLOAD_TEMPLATE_STATUS.clear()

    def test_parse_site_inventory_has_expected_size(self):
        inventory = module.parse_site_inventory()
        self.assertEqual(len(inventory), 83)
        self.assertEqual(inventory[0]["site_id"], "JP-Ozm")
        self.assertEqual(inventory[-1]["site_id"], "JP-Tmd")

    def test_collect_measurement_years_ignores_era5_files(self):
        entries = [
            {"name": "FLX_JP-Ozm_JapanFLUX2024_ALLVARS_HH_2015-2017_1-3.csv", "directory": False},
            {"name": "FLX_JP-Ozm_JapanFLUX2024_ALLVARS_MM_2015-2017_1-3.csv", "directory": False},
            {"name": "FLX_JP-Ozm_JapanFLUX2024_ERA5_DD_1990-2024_1-3.csv", "directory": False},
        ]

        first_year, last_year = module.collect_measurement_years(entries, "JP-Ozm")

        self.assertEqual((first_year, last_year), (2015, 2017))

    def test_build_site_row_uses_landing_page_when_direct_url_missing(self):
        row = module.build_site_row(
            {
                "metadata_id": "A20240722-001",
                "site_id": "JP-Ozm",
                "site_name": "Oizumi Urban Park",
                "country": "JP",
                "vegetation_type": "URB",
                "latitude": 34.56347,
                "longitude": 135.533484,
            },
            "1.00",
            2015,
            2017,
            "",
        )

        self.assertEqual(row["download_mode"], "landing_page")
        self.assertEqual(row["download_link"], "https://ads.nipr.ac.jp/dataset/A20240722-001")
        self.assertEqual(row["direct_download_url"], "")
        self.assertEqual(row["processing_lineage"], "other_processed")

    def test_validate_direct_download_url_returns_first_resolved_candidate(self):
        with mock.patch.object(module, "probe_direct_download_url", side_effect=[None, "", "https://example.org/japanflux.zip"]):
            resolved = module.validate_direct_download_url(
                "A20240722-001",
                "1.00",
                "JapanFlux/A20240722-001/v100/dataset_zip",
                timeout=5,
            )

        self.assertEqual(resolved, "https://example.org/japanflux.zip")

    def test_validate_direct_download_url_falls_back_when_no_candidate_resolves(self):
        with mock.patch.object(module, "probe_direct_download_url", return_value=None):
            resolved = module.validate_direct_download_url(
                "A20240722-001",
                "1.00",
                "JapanFlux/A20240722-001/v100/dataset_zip",
                timeout=5,
            )

        self.assertEqual(resolved, "")


if __name__ == "__main__":
    unittest.main()
