import unittest

from scripts import refresh_efd_sites as module


class RefreshEfdSitesTests(unittest.TestCase):
    def test_split_html_tokens_handles_breaks_and_deduplicates(self):
        tokens = module.split_html_tokens("Public<br />Private<br>Public")
        self.assertEqual(tokens, ["Public", "Private"])

    def test_build_site_row_uses_public_request_workflow(self):
        row = module.build_site_row(
            {
                "Code": "BE-Bra",
                "Name": "Brasschaat",
                "Igbp": "MF",
                "Latitude": 51.30761,
                "Longitude": 4.51984,
                "FluxList": "CO2-E<br />LE-E<br />",
                "Access": "Public<br />Private",
                "DataUse": "Open<br />Close",
                "Networks": "EuroFlux<br />ICOS",
            },
            "2026-04-02T00:00:00Z",
        )

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["site_id"], "BE-Bra")
        self.assertEqual(row["country"], "BE")
        self.assertEqual(row["data_hub"], "EFD")
        self.assertEqual(row["download_mode"], "request_page")
        self.assertEqual(row["download_link"], module.REQUEST_PAGE_URL)
        self.assertEqual(row["source_origin"], "efd")
        self.assertEqual(row["network"], "EuroFlux; ICOS")
        self.assertEqual(row["flux_list"], "CO2-E; LE-E")
        self.assertEqual(row["access_label"], "Public; Private")
        self.assertEqual(row["data_use_label"], "Open; Close")
        self.assertEqual(row["processing_lineage"], "")

    def test_parse_public_site_json_uses_structured_fallback_lists(self):
        rows = module.parse_public_site_json(
            {
                "d": [
                    {
                        "Code": "DE-Foo",
                        "Name": "Foo Forest",
                        "Igbp": "ENF",
                        "Latitude": 50.0,
                        "Longitude": 8.0,
                        "FluxList": "",
                        "Access": "Public",
                        "DataUse": "Open",
                        "Networks": "",
                        "lNet": [{"Name": "CarboEuropeIP"}],
                        "mFlux": [{"Name": "CO2"}, {"Name": "LE"}],
                    }
                ]
            },
            "2026-04-02T00:00:00Z",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["site_id"], "DE-Foo")
        self.assertEqual(rows[0]["network"], "CarboEuropeIP")
        self.assertEqual(rows[0]["flux_list"], "CO2; LE")

    def test_parse_public_site_csv_builds_partial_rows_when_json_is_unavailable(self):
        rows = module.parse_public_site_csv(
            "\n".join(
                [
                    "Site Code,Site Name,Site Latitude,Site Longitude,IGBP Code,Fluxes",
                    "FI-Bar,Bar Wetland,61.1,24.2,WET,CO2-E|LE-E",
                ]
            ),
            "2026-04-02T00:00:00Z",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["site_id"], "FI-Bar")
        self.assertEqual(rows[0]["download_mode"], "request_page")
        self.assertEqual(rows[0]["flux_list"], "CO2-E; LE-E")
        self.assertEqual(rows[0]["access_label"], "")
        self.assertEqual(rows[0]["data_use_label"], "")

    def test_dedupe_site_rows_prefers_more_complete_record(self):
        rows = module.dedupe_site_rows(
            [
                {
                    "site_id": "FR-Test",
                    "site_name": "Test",
                    "network": "",
                    "vegetation_type": "",
                    "flux_list": "",
                    "access_label": "",
                    "data_use_label": "",
                    "latitude": "",
                    "longitude": "",
                },
                {
                    "site_id": "FR-Test",
                    "site_name": "Test",
                    "network": "ICOS",
                    "vegetation_type": "DBF",
                    "flux_list": "CO2-E",
                    "access_label": "Public",
                    "data_use_label": "Open",
                    "latitude": "48.0",
                    "longitude": "2.0",
                },
            ]
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["network"], "ICOS")
        self.assertEqual(rows[0]["access_label"], "Public")


if __name__ == "__main__":
    unittest.main()
