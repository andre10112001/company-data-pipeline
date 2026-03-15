"""
test_pipeline.py
----------------
Unit tests for the enrich() function in pipeline.py.

The tests use synthetic (fake) data to verify the join/enrichment
logic in isolation — no real JSON files are needed to run these tests.

Run with:
    pytest test_pipeline.py -v
"""

import pandas as pd
import pytest
from pipeline import enrich


# ---------------------------------------------------------------------------
# Shared test fixtures
# Fixtures are reusable pieces of test data that pytest injects
# automatically into any test function that requests them by name.
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_tree() -> pd.DataFrame:
    """
    A minimal family tree with 3 members:
    - Root company (level 1, no parent)
    - Subsidiary A (level 2, parent = root)
    - Subsidiary B (level 2, parent = root)
    """
    return pd.DataFrame([
        {
            "duns": "100000001",
            "primary_name": "Root Company Inc.",
            "hierarchy_level": 1,
            "parent_duns": None,
            "global_ultimate_duns": "100000001",
            "country": "United States",
        },
        {
            "duns": "200000001",
            "primary_name": "Subsidiary A Ltd.",
            "hierarchy_level": 2,
            "parent_duns": "100000001",
            "global_ultimate_duns": "100000001",
            "country": "United Kingdom",
        },
        {
            "duns": "200000002",
            "primary_name": "Subsidiary B GmbH",
            "hierarchy_level": 3,
            "parent_duns": "200000001",
            "global_ultimate_duns": "100000001",
            "country": "Germany",
        },
    ])


@pytest.fixture
def sample_root_profile() -> dict:
    """
    A minimal root company profile from data_blocks.json.
    Represents the enrichment data attached to every tree member.
    """
    return {
        "root_duns": "100000001",
        "root_primary_name": "Root Company Inc.",
        "root_yearly_revenue": 5000000.0,
        "root_currency": "USD",
        "root_industry_description": "Software Publishers",
        "root_operating_status": "Active",
        "root_address_country": "United States",
        "root_employees_hq": 200,
        "root_employees_consolidated": 1500,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestEnrich:
    """Tests for the enrich() join/enrichment function."""

    def test_output_row_count_matches_tree(self, sample_tree, sample_root_profile):
        """
        The enriched DataFrame must have the same number of rows
        as the input family tree — no rows should be lost or duplicated
        during the join.
        """
        df_enriched = enrich(sample_tree, sample_root_profile)
        assert len(df_enriched) == len(sample_tree), (
            f"Expected {len(sample_tree)} rows after enrichment "
            f"but got {len(df_enriched)}."
        )

    def test_root_profile_attached_to_all_members(self, sample_tree, sample_root_profile):
        """
        Every family tree member should have the root company's profile
        attached after enrichment (Enrichment 1).
        Since all members share the same global_ultimate_duns, all rows
        should have root_primary_name populated.
        """
        df_enriched = enrich(sample_tree, sample_root_profile)
        null_count = df_enriched["root_primary_name"].isna().sum()
        assert null_count == 0, (
            f"Expected all rows to have root_primary_name populated "
            f"but {null_count} rows are missing it."
        )

    def test_root_profile_values_are_correct(self, sample_tree, sample_root_profile):
        """
        The values attached from the root profile must match
        the original root profile data exactly.
        """
        df_enriched = enrich(sample_tree, sample_root_profile)
        for _, row in df_enriched.iterrows():
            assert row["root_yearly_revenue"] == 5000000.0
            assert row["root_currency"] == "USD"
            assert row["root_industry_description"] == "Software Publishers"
            assert row["root_operating_status"] == "Active"

    def test_parent_name_attached_to_subsidiaries(self, sample_tree, sample_root_profile):
        """
        Every non-root member should have their direct parent's name
        attached after enrichment (Enrichment 2).
        """
        df_enriched = enrich(sample_tree, sample_root_profile)

        # Non-root members (hierarchy_level > 1) should have parent_name
        non_root = df_enriched[df_enriched["hierarchy_level"] > 1]
        null_parent_names = non_root["parent_name"].isna().sum()
        assert null_parent_names == 0, (
            f"Expected all non-root members to have parent_name "
            f"but {null_parent_names} are missing it."
        )

    def test_root_member_has_no_parent_name(self, sample_tree, sample_root_profile):
        """
        The root member (hierarchy_level == 1) has no parent,
        so parent_name should be null after enrichment.
        """
        df_enriched = enrich(sample_tree, sample_root_profile)
        root_row = df_enriched[df_enriched["hierarchy_level"] == 1]
        assert root_row["parent_name"].isna().all(), (
            "Root member should have null parent_name but it has a value."
        )

    def test_parent_name_values_are_correct(self, sample_tree, sample_root_profile):
        """
        Verify the actual parent name values are correct —
        Subsidiary A's parent should be Root Company Inc.,
        Subsidiary B's parent should be Subsidiary A Ltd.
        """
        df_enriched = enrich(sample_tree, sample_root_profile)

        sub_a = df_enriched[df_enriched["duns"] == "200000001"].iloc[0]
        sub_b = df_enriched[df_enriched["duns"] == "200000002"].iloc[0]

        assert sub_a["parent_name"] == "Root Company Inc.", (
            f"Subsidiary A's parent should be 'Root Company Inc.' "
            f"but got '{sub_a['parent_name']}'."
        )
        assert sub_b["parent_name"] == "Subsidiary A Ltd.", (
            f"Subsidiary B's parent should be 'Subsidiary A Ltd.' "
            f"but got '{sub_b['parent_name']}'."
        )

    def test_left_join_keeps_unmatched_members(self, sample_root_profile):
        """
        If a tree member has a global_ultimate_duns that does NOT match
        the root profile, the row should still be kept (LEFT JOIN behaviour)
        but root profile fields should be null.
        """
        # Tree with one member pointing to a DIFFERENT global ultimate
        df_unmatched_tree = pd.DataFrame([
            {
                "duns": "999999999",
                "primary_name": "Orphan Company",
                "hierarchy_level": 2,
                "parent_duns": "888888888",
                "global_ultimate_duns": "888888888",  # does not match root profile
                "country": "France",
            }
        ])

        df_enriched = enrich(df_unmatched_tree, sample_root_profile)

        # Row should still be there
        assert len(df_enriched) == 1, "Unmatched row should not be dropped."

        # Root profile fields should be null
        assert pd.isna(df_enriched.iloc[0]["root_primary_name"]), (
            "Unmatched row should have null root_primary_name."
        )

    def test_output_contains_expected_columns(self, sample_tree, sample_root_profile):
        """
        The enriched DataFrame should contain all original tree columns
        plus the root profile columns and parent_name.
        """
        df_enriched = enrich(sample_tree, sample_root_profile)

        expected_columns = [
            # Original tree columns
            "duns", "primary_name", "hierarchy_level",
            "parent_duns", "global_ultimate_duns", "country",
            # Enrichment 1 — root profile
            "root_duns", "root_primary_name", "root_yearly_revenue",
            "root_industry_description", "root_operating_status",
            # Enrichment 2 — parent name
            "parent_name",
        ]

        for col in expected_columns:
            assert col in df_enriched.columns, (
                f"Expected column '{col}' not found in enriched DataFrame."
            )