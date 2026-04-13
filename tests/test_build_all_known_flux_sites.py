import unittest
from pathlib import Path
from typing import Optional

from scripts import build_all_known_flux_sites as module


def make_spec(
    *,
    source_system: str = "external_docs",
    default_source_network: str = "Test source",
    precedence: int = 20,
    in_explorer: bool = False,
    has_accessible_data: bool = False,
) -> module.SourceSpec:
    return module.SourceSpec(
        path=Path("/tmp/test.csv"),
        display_path="test.csv",
        source_system=source_system,
        default_source_network=default_source_network,
        precedence=precedence,
        in_explorer=in_explorer,
        has_accessible_data=has_accessible_data,
    )


def make_record(fields: dict[str, str], spec: Optional[module.SourceSpec] = None) -> module.RawSiteRecord:
    record = module.build_raw_record(
        module.row_to_normalized_fields(fields),
        spec or make_spec(),
        row_number=2,
        sheet_name="",
    )
    if record is None:
        raise AssertionError("Expected a normalized record")
    return record


def make_accessible_truth(*site_ids: str) -> module.ExplorerAccessibleTruth:
    truth = module.ExplorerAccessibleTruth()
    for site_id in site_ids:
        truth.add_site_identity(site_id=site_id)
    return truth


class BuildAllKnownFluxSitesTests(unittest.TestCase):
    def test_column_normalization_maps_common_aliases(self):
        record = make_record(
            {
                "Site ID": "US-Var",
                "Station Name": "Vaira Ranch",
                "Country Name": "United States",
                "Lat": "38.4133",
                "Long": "-120.9508",
                "Region": "AmeriFlux",
            }
        )

        self.assertEqual(record.site_id, "US-Var")
        self.assertEqual(record.site_code, "Var")
        self.assertEqual(record.site_name, "Vaira Ranch")
        self.assertEqual(record.country_code, "US")
        self.assertEqual(record.country, "United States")
        self.assertAlmostEqual(record.latitude or 0.0, 38.4133)
        self.assertAlmostEqual(record.longitude or 0.0, -120.9508)
        self.assertEqual(record.source_network, "AmeriFlux")

    def test_duplicate_merging_prefers_exact_site_id(self):
        accessible_spec = make_spec(
            source_system="explorer_accessible",
            default_source_network="FLUXNET-Shuttle",
            precedence=100,
            in_explorer=True,
            has_accessible_data=True,
        )
        external_spec = make_spec(default_source_network="Pingyu")

        records = [
            make_record(
                {
                    "site_id": "US-Var",
                    "site_name": "Vaira Ranch",
                    "country_code": "US",
                    "latitude": "38.4133",
                    "longitude": "-120.9508",
                },
                accessible_spec,
            ),
            make_record(
                {
                    "Site_ID": "US-VAR",
                    "Site_Name": "Vaira Ranch (external)",
                    "lat": "38.41331",
                    "lon": "-120.95079",
                },
                external_spec,
            ),
        ]

        groups = module.merge_records(records)
        canonical_sites, review_rows = module.build_canonical_sites(groups)

        self.assertEqual(len(canonical_sites), 1)
        self.assertEqual(canonical_sites[0]["site_id"], "US-Var")
        self.assertTrue(canonical_sites[0]["in_explorer"])
        self.assertTrue(canonical_sites[0]["has_accessible_data"])
        self.assertFalse(canonical_sites[0]["known_site_only"])
        self.assertEqual(review_rows, [])

    def test_duplicate_merging_uses_country_code_plus_site_code(self):
        accessible_spec = make_spec(
            source_system="explorer_accessible",
            default_source_network="ICOS",
            precedence=100,
            in_explorer=True,
            has_accessible_data=True,
        )

        records = [
            make_record(
                {
                    "site_id": "US-Var",
                    "site_name": "Vaira Ranch",
                    "country_code": "US",
                    "latitude": "38.4133",
                    "longitude": "-120.9508",
                },
                accessible_spec,
            ),
            make_record(
                {
                    "tower_code": "Var",
                    "station_name": "Vaira Ranch field list",
                    "country": "United States",
                    "lat": "38.4134",
                    "lng": "-120.9507",
                }
            ),
        ]

        groups = module.merge_records(records)
        canonical_sites, _ = module.build_canonical_sites(groups)

        self.assertEqual(len(canonical_sites), 1)
        self.assertEqual(canonical_sites[0]["site_id"], "US-Var")
        self.assertEqual(canonical_sites[0]["site_code"], "Var")

    def test_ambiguous_country_code_plus_site_code_is_left_unmerged(self):
        records = [
            make_record(
                {
                    "site_id": "US-Var",
                    "site_name": "Vaira Ranch",
                    "country_code": "US",
                    "latitude": "38.4133",
                    "longitude": "-120.9508",
                }
            ),
            make_record(
                {
                    "tower_code": "Var",
                    "site_name": "Vaira Ranch duplicate?",
                    "country_code": "US",
                    "latitude": "40.0000",
                    "longitude": "-100.0000",
                }
            ),
        ]

        groups = module.merge_records(records)
        canonical_sites, review_rows = module.build_canonical_sites(groups)

        self.assertEqual(len(canonical_sites), 2)
        self.assertEqual(len(review_rows), 1)
        self.assertIn("conflicting coordinates", review_rows[0]["review_reason"])

    def test_country_code_inference_from_country_name(self):
        record = make_record(
            {
                "site_code": "Bal",
                "site_name": "Balcarce BA",
                "country": "Argentina",
                "lat": "-37.7596",
                "lon": "-58.3024",
            }
        )

        self.assertEqual(record.country_code, "AR")
        self.assertEqual(record.country, "Argentina")

    def test_known_site_only_flag_is_preserved_for_external_only_sites(self):
        external_only = make_record(
            {
                "Site_ID": "JP-AKO",
                "Site_Name": "Ako reference tower",
                "Country": "Japan",
                "lat": "34.786316",
                "lon": "134.370861",
            }
        )

        accessible_and_external = [
            make_record(
                {
                    "site_id": "US-Var",
                    "site_name": "Vaira Ranch",
                    "country_code": "US",
                    "latitude": "38.4133",
                    "longitude": "-120.9508",
                },
                make_spec(
                    source_system="explorer_accessible",
                    default_source_network="FLUXNET-Shuttle",
                    precedence=100,
                    in_explorer=True,
                    has_accessible_data=True,
                ),
            ),
            make_record(
                {
                    "site_id": "US-Var",
                    "site_name": "Vaira Ranch copy",
                    "country": "United States",
                    "lat": "38.41331",
                    "lon": "-120.95081",
                }
            ),
        ]

        groups = module.merge_records([external_only] + accessible_and_external)
        canonical_sites, _ = module.build_canonical_sites(groups)
        sites_by_id = {row["site_id"]: row for row in canonical_sites}

        self.assertTrue(sites_by_id["JP-AKO"]["known_site_only"])
        self.assertFalse(sites_by_id["JP-AKO"]["has_accessible_data"])
        self.assertFalse(sites_by_id["JP-AKO"]["in_explorer"])

        self.assertFalse(sites_by_id["US-Var"]["known_site_only"])
        self.assertTrue(sites_by_id["US-Var"]["has_accessible_data"])
        self.assertTrue(sites_by_id["US-Var"]["in_explorer"])

    def test_fluxnet2015_backed_accessible_site_is_not_known_site_only(self):
        groups = module.merge_records(
            [
                make_record(
                    {
                        "site_id": "US-BLO",
                        "site_name": "Blodgett Forest",
                        "country": "United States",
                        "latitude": "38.8953",
                        "longitude": "-120.6328",
                    },
                    make_spec(
                        source_system="explorer_repo_metadata",
                        default_source_network="FLUXNET2015",
                        precedence=50,
                    ),
                )
            ]
        )

        canonical_sites, _ = module.build_canonical_sites(
            groups,
            accessible_truth=make_accessible_truth("US-Blo"),
        )

        self.assertEqual(len(canonical_sites), 1)
        self.assertEqual(canonical_sites[0]["site_id"], "US-BLO")
        self.assertTrue(canonical_sites[0]["has_accessible_data"])
        self.assertTrue(canonical_sites[0]["in_explorer"])
        self.assertFalse(canonical_sites[0]["known_site_only"])

    def test_accessible_truth_matches_country_plus_site_code_when_site_id_is_missing(self):
        groups = module.merge_records(
            [
                make_record(
                    {
                        "tower_code": "Blo",
                        "station_name": "Blodgett Forest",
                        "country": "United States",
                        "latitude": "38.8953",
                        "longitude": "-120.6328",
                    }
                )
            ]
        )

        canonical_sites, _ = module.build_canonical_sites(
            groups,
            accessible_truth=make_accessible_truth("US-Blo"),
        )

        self.assertEqual(len(canonical_sites), 1)
        self.assertEqual(canonical_sites[0]["site_code"], "Blo")
        self.assertTrue(canonical_sites[0]["has_accessible_data"])
        self.assertTrue(canonical_sites[0]["in_explorer"])
        self.assertFalse(canonical_sites[0]["known_site_only"])

    def test_known_site_only_implies_no_accessible_data_with_accessible_truth(self):
        groups = module.merge_records(
            [
                make_record(
                    {
                        "Site_ID": "JP-AKO",
                        "Site_Name": "Ako reference tower",
                        "Country": "Japan",
                        "lat": "34.786316",
                        "lon": "134.370861",
                    }
                ),
                make_record(
                    {
                        "site_id": "US-BLO",
                        "site_name": "Blodgett Forest",
                        "country": "United States",
                        "latitude": "38.8953",
                        "longitude": "-120.6328",
                    }
                ),
            ]
        )

        canonical_sites, _ = module.build_canonical_sites(
            groups,
            accessible_truth=make_accessible_truth("US-Blo"),
        )

        self.assertEqual(len(canonical_sites), 2)
        for site in canonical_sites:
            if site["known_site_only"]:
                self.assertFalse(site["has_accessible_data"])


if __name__ == "__main__":
    unittest.main()
