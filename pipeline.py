"""
pipeline.py
-----------
Ingests data_blocks.json and family_tree.json for each company,
enriches family tree members with root company profile and parent name,
validates fields, and saves one Parquet file per company.

Usage:
    python pipeline.py --data_dir ./data --output_dir ./output
"""

import os
import json
import logging
import argparse
from typing import Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Load JSON
# ---------------------------------------------------------------------------

def load_json(filepath: str) -> Optional[dict]:
    """
    Reads a JSON file and returns its contents as a dict.
    Returns None if the file is missing or malformed.
    """
    if not os.path.exists(filepath):
        logger.error(f"File not found: {filepath}")
        return None
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Loaded: {filepath}")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"Malformed JSON in {filepath}: {e}")
        return None


# ---------------------------------------------------------------------------
# 2. Parse data_blocks.json
# ---------------------------------------------------------------------------

def parse_data_blocks(data: dict) -> Optional[dict]:
    """
    Extracts relevant fields from data_blocks.json.
    Returns a flat dict representing the root company profile.
    Returns None if the critical field (duns) is missing.
    """
    if not data:
        return None

    duns = data.get("duns")
    if not duns:
        logger.error("data_blocks.json is missing 'duns' — skipping company.")
        return None

    # --- Financials: take the first available yearly revenue ---
    revenue = None
    currency = None
    statement_date = None
    financials = data.get("financials", [])
    if financials:
        first = financials[0]
        yearly = first.get("yearlyRevenue", [])
        if yearly:
            revenue = yearly[0].get("value")
            currency = yearly[0].get("currency")
        statement_date = first.get("financialStatementToDate")
    else:
        logger.warning(f"[{duns}] No financials found in data_blocks.")

    # --- Employees: separate HQ-only and consolidated ---
    employees_hq = None
    employees_consolidated = None
    for emp in data.get("numberOfEmployees", []):
        scope = emp.get("informationScopeDescription", "")
        if "Consolidated" in scope:
            employees_consolidated = emp.get("value")
        elif "Headquarters" in scope:
            employees_hq = emp.get("value")

    # --- Primary address ---
    addr = data.get("primaryAddress", {})
    address_street     = addr.get("streetAddress", {}).get("line1")
    address_city       = addr.get("addressLocality", {}).get("name")
    address_region     = addr.get("addressRegion", {}).get("name")
    address_country    = addr.get("addressCountry", {}).get("name")
    address_country_iso = addr.get("addressCountry", {}).get("isoAlpha2Code")
    address_postal     = addr.get("postalCode")

    # --- Corporate linkage ---
    cl = data.get("corporateLinkage", {})
    hierarchy_level       = cl.get("hierarchyLevel")
    global_ultimate_duns  = cl.get("globalUltimate", {}).get("duns")
    domestic_ultimate_duns = cl.get("domesticUltimate", {}).get("duns")

    # --- Operating status ---
    status = (
        data.get("dunsControlStatus", {})
            .get("operatingStatus", {})
            .get("description")
    )

    # --- Primary industry ---
    primary_industry_sic  = data.get("primaryIndustryCode", {}).get("usSicV4")
    primary_industry_desc = data.get("primaryIndustryCode", {}).get("usSicV4Description")

    return {
        # Identity
        "root_duns":                 duns,
        "root_primary_name":         data.get("primaryName"),
        "root_registered_name":      data.get("registeredName"),
        "root_start_date":           data.get("startDate"),
        "root_incorporated_date":    data.get("incorporatedDate"),
        "root_entity_type":          data.get("businessEntityType", {}).get("description"),
        "root_ownership_type":       data.get("controlOwnershipType", {}).get("description"),
        "root_is_standalone":        data.get("isStandalone"),
        "root_is_fortune1000":       data.get("isFortune1000Listed"),
        "root_operating_status":     status,
        "root_is_marketable":        data.get("dunsControlStatus", {}).get("isMarketable"),
        # Industry
        "root_industry_sic":         primary_industry_sic,
        "root_industry_description": primary_industry_desc,
        # Address
        "root_address_street":       address_street,
        "root_address_city":         address_city,
        "root_address_region":       address_region,
        "root_address_country":      address_country,
        "root_address_country_iso":  address_country_iso,
        "root_address_postal":       address_postal,
        # Financials
        "root_yearly_revenue":       revenue,
        "root_currency":             currency,
        "root_statement_date":       statement_date,
        # Employees
        "root_employees_hq":         employees_hq,
        "root_employees_consolidated": employees_consolidated,
        # Hierarchy
        "root_hierarchy_level":      hierarchy_level,
        "root_global_ultimate_duns": global_ultimate_duns,
        "root_domestic_ultimate_duns": domestic_ultimate_duns,
    }


# ---------------------------------------------------------------------------
# 3. Parse family_tree.json
# ---------------------------------------------------------------------------

def parse_family_tree(data: dict) -> Optional[pd.DataFrame]:
    """
    Extracts all family tree members from family_tree.json.
    Returns a DataFrame with one row per member.
    Returns None if the file is empty or members list is missing.
    """
    if not data:
        return None

    members = data.get("familyTreeMembers", [])
    if not members:
        logger.warning("family_tree.json has no familyTreeMembers.")
        return None

    global_ultimate_duns = data.get("globalUltimateDuns")
    rows = []

    for member in members:
        duns = member.get("duns")
        if not duns:
            logger.warning("Family tree member missing 'duns' — skipping row.")
            continue

        cl = member.get("corporateLinkage", {})
        parent_duns     = cl.get("parent", {}).get("duns")
        hierarchy_level = cl.get("hierarchyLevel")
        roles           = [r.get("description") for r in cl.get("familytreeRolesPlayed", [])]

        addr = member.get("primaryAddress", {})
        country  = addr.get("addressCountry", {}).get("name")
        city     = addr.get("addressLocality", {}).get("name")
        region   = addr.get("addressRegion", {}).get("name")
        postal   = addr.get("postalCode")

        # Revenue and employees at member level
        fins = member.get("financials", [])
        member_revenue = None
        if fins:
            yearly = fins[0].get("yearlyRevenues", [])
            if yearly:
                member_revenue = yearly[0].get("value")

        emps = member.get("numberOfEmployees", [])
        member_employees = emps[0].get("value") if emps else None

        rows.append({
            "duns":                  duns,
            "primary_name":          member.get("primaryName"),
            "start_date":            member.get("startDate"),
            "hierarchy_level":       hierarchy_level,
            "parent_duns":           parent_duns,
            "global_ultimate_duns":  global_ultimate_duns,
            "roles":                 ", ".join(roles) if roles else None,
            "country":               country,
            "city":                  city,
            "region":                region,
            "postal_code":           postal,
            "member_yearly_revenue": member_revenue,
            "member_employees":      member_employees,
        })

    logger.info(f"Parsed {len(rows)} family tree members.")
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 4. Enrich — join data_blocks profile onto family tree members
# ---------------------------------------------------------------------------

def enrich(df_tree: pd.DataFrame, root_profile: dict) -> pd.DataFrame:
    """
    Enrichment 1: Attach root company profile to every family tree member
                  by matching on global_ultimate_duns.
    Enrichment 2: Attach parent company name to every member
                  by self-joining the tree on parent_duns.
    """
    # --- Enrichment 1: root company profile ---
    df_root = pd.DataFrame([root_profile])
    df_enriched = df_tree.merge(
        df_root,
        left_on="global_ultimate_duns",
        right_on="root_duns",
        how="left"
    )

    matched = df_enriched["root_duns"].notna().sum()
    logger.info(
        f"Enrichment 1 (root profile): {matched}/{len(df_enriched)} rows matched."
    )

    # --- Enrichment 2: parent name (self-join on parent_duns) ---
    parent_names = df_tree[["duns", "primary_name"]].rename(columns={
        "duns": "parent_duns",
        "primary_name": "parent_name"
    })
    df_enriched = df_enriched.merge(parent_names, on="parent_duns", how="left")

    parent_matched = df_enriched["parent_name"].notna().sum()
    logger.info(
        f"Enrichment 2 (parent name): {parent_matched}/{len(df_enriched)} rows matched."
    )

    return df_enriched


# ---------------------------------------------------------------------------
# 5. Validate
# ---------------------------------------------------------------------------

def validate(df: pd.DataFrame, company_name: str) -> pd.DataFrame:
    """
    Runs data quality checks on the enriched DataFrame.
    Logs warnings for issues found.
    Does NOT drop rows — flags them with a 'data_quality_flag' column instead.
    """
    flags = []

    # Check 1: missing duns
    missing_duns = df["duns"].isna().sum()
    if missing_duns > 0:
        logger.warning(f"[{company_name}] {missing_duns} rows with missing 'duns'.")
        flags.append("missing_duns")

    # Check 2: missing primary_name
    missing_name = df["primary_name"].isna().sum()
    if missing_name > 0:
        logger.warning(f"[{company_name}] {missing_name} rows with missing 'primary_name'.")

    # Check 3: missing hierarchy_level
    missing_level = df["hierarchy_level"].isna().sum()
    if missing_level > 0:
        logger.warning(f"[{company_name}] {missing_level} rows with missing 'hierarchy_level'.")

    # Check 4: root members should have no parent_duns
    root_with_parent = df[
        (df["hierarchy_level"] == 1) & (df["parent_duns"].notna())
    ]
    if not root_with_parent.empty:
        logger.warning(
            f"[{company_name}] {len(root_with_parent)} root-level members "
            f"unexpectedly have a parent_duns."
        )

    # Check 5: non-root members should have a parent_duns
    orphans = df[
        (df["hierarchy_level"] > 1) & (df["parent_duns"].isna())
    ]
    if not orphans.empty:
        logger.warning(
            f"[{company_name}] {len(orphans)} non-root members are missing parent_duns."
        )

    # Check 6: duplicate duns
    dupes = df["duns"].duplicated().sum()
    if dupes > 0:
        logger.warning(f"[{company_name}] {dupes} duplicate 'duns' values found.")

    # Summary
    total = len(df)
    logger.info(
        f"[{company_name}] Validation complete — {total} rows, "
        f"{df.isna().sum().sum()} total null values across all columns."
    )

    return df


# ---------------------------------------------------------------------------
# 6. Save to Parquet
# ---------------------------------------------------------------------------

def save_parquet(df: pd.DataFrame, output_path: str) -> None:
    """
    Saves the enriched DataFrame to a Parquet file.
    Creates the output directory if it doesn't exist.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False, engine="pyarrow")
    logger.info(f"Saved Parquet: {output_path} ({len(df)} rows, {df.shape[1]} columns)")


# ---------------------------------------------------------------------------
# 7. Process one company folder
# ---------------------------------------------------------------------------

def process_company(folder_path: str, output_dir: str) -> bool:
    """
    Orchestrates the full pipeline for one company folder.
    Returns True if successful, False if a critical error occurred.
    """
    company_name = os.path.basename(folder_path)
    logger.info(f"{'='*60}")
    logger.info(f"Processing company: {company_name}")
    logger.info(f"{'='*60}")

    # --- Step 1: Load ---
    data_blocks_path  = os.path.join(folder_path, "data_blocks.json")
    family_tree_path  = os.path.join(folder_path, "family_tree.json")

    data_blocks  = load_json(data_blocks_path)
    family_tree  = load_json(family_tree_path)

    if data_blocks is None or family_tree is None:
        logger.error(f"[{company_name}] Skipping — one or both source files failed to load.")
        return False

    # --- Step 2: Parse ---
    root_profile = parse_data_blocks(data_blocks)
    if root_profile is None:
        logger.error(f"[{company_name}] Skipping — failed to parse data_blocks.json.")
        return False

    df_tree = parse_family_tree(family_tree)
    if df_tree is None or df_tree.empty:
        logger.error(f"[{company_name}] Skipping — failed to parse family_tree.json.")
        return False

    # --- Step 3: Enrich ---
    df_enriched = enrich(df_tree, root_profile)

    # --- Step 4: Validate ---
    df_validated = validate(df_enriched, company_name)

    # --- Step 5: Save ---
    safe_name   = company_name.lower().replace(" ", "_")
    output_path = os.path.join(output_dir, f"{safe_name}.parquet")
    save_parquet(df_validated, output_path)

    return True


# ---------------------------------------------------------------------------
# 8. Main — loop over all company folders
# ---------------------------------------------------------------------------

def main(data_dir: str, output_dir: str) -> None:
    """
    Entry point. Discovers all company subfolders in data_dir
    and runs the pipeline for each one.
    """
    if not os.path.exists(data_dir):
        logger.error(f"Data directory not found: {data_dir}")
        return

    # Discover company folders
    company_folders = [
        os.path.join(data_dir, d)
        for d in sorted(os.listdir(data_dir))
        if os.path.isdir(os.path.join(data_dir, d))
    ]

    if not company_folders:
        logger.error(f"No company folders found in: {data_dir}")
        return

    logger.info(f"Found {len(company_folders)} company folder(s): "
                f"{[os.path.basename(f) for f in company_folders]}")

    # Process each company
    results = {"success": 0, "failed": 0}
    for folder in company_folders:
        success = process_company(folder, output_dir)
        if success:
            results["success"] += 1
        else:
            results["failed"] += 1

    # Final summary
    logger.info(f"{'='*60}")
    logger.info(
        f"Pipeline complete — "
        f"{results['success']} succeeded, {results['failed']} failed."
    )
    logger.info(f"Output files saved to: {output_dir}")
    logger.info(f"{'='*60}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Company data pipeline")
    parser.add_argument(
        "--data_dir",
        type=str,
        default="./data",
        help="Path to the folder containing company subfolders"
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="./output",
        help="Path to the folder where Parquet files will be saved"
    )
    args = parser.parse_args()
    main(args.data_dir, args.output_dir)