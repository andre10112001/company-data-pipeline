# Company Data Pipeline — README

## Project Overview

This pipeline ingests and processes company data from two D&B (Dun & Bradstreet) source files — `data_blocks.json` and `family_tree.json` — enriches the data by attaching parent-child corporate relationships, and outputs the result as a Parquet file for downstream analysis.

---

## Data Sources

### `data_blocks.json` — Company Profile
A detailed profile for a single company pulled from the D&B API. Contains identity, address, industry classification, financials, employee counts, legal information, and corporate linkage metadata. One file = one root company.

### `family_tree.json` — Corporate Hierarchy
A list of all legal entities belonging to the same corporate group as the root company. Each member includes its position in the hierarchy (`hierarchy_level`), its direct parent (`parent.duns`), and its direct children (`children[].duns`). One file = up to 1,000 members per page (branches excluded).

---

## Database Schema

The relational schema is designed to store the data from both JSON files in a normalised, queryable structure.

📎 **[View the full ERD and schema diagram here →](https://dbdiagram.io)**
*(Paste the schema code from `schema.dbml` into dbdiagram.io to render the diagram)*

### Tables

| Table | Source | Description |
|---|---|---|
| `companies` | `data_blocks.json` | Core identity and profile for each root company. One row per company. |
| `addresses` | `data_blocks.json` | Normalised addresses. A company can have up to three address types: primary, registered, and mailing. |
| `industry_codes` | `data_blocks.json` | Industry classifications. A company can have many codes across different systems (NAICS, SIC, NACE, D&B). |
| `financials` | `data_blocks.json` | Yearly revenue and balance sheet snapshots. Multiple records per company over time. |
| `employees` | `data_blocks.json` | Employee counts by scope. Headquarters-only and consolidated counts are stored as separate rows. |
| `family_tree_members` | `family_tree.json` | All legal entities in the corporate group. Includes hierarchy level, parent DUNS, and basic identity. |
| `tree_roles` | `family_tree.json` | Roles played by each member in the hierarchy (e.g. Global Ultimate, Subsidiary, Parent/HQ). One member can hold multiple roles simultaneously. |

---

## Queries This Schema Is Designed to Answer

### Corporate Hierarchy

**Who is the ultimate parent (Global Ultimate) of a given company?**
```sql
SELECT global_ultimate_duns, primary_name
FROM family_tree_members
WHERE duns = '199882911';
```

**What are all the direct subsidiaries of a given company?**
```sql
SELECT duns, primary_name, hierarchy_level
FROM family_tree_members
WHERE parent_duns = '199882911'
ORDER BY primary_name;
```

**How deep in the corporate hierarchy is a given entity?**
```sql
SELECT duns, primary_name, hierarchy_level
FROM family_tree_members
WHERE duns = '079746429';
```

**What is the full ownership chain from a subsidiary up to the root?**
```sql
-- Recursive CTE: walks up the tree from child to root
WITH RECURSIVE hierarchy AS (
    SELECT duns, primary_name, parent_duns, hierarchy_level
    FROM family_tree_members
    WHERE duns = '079746429'          -- start from the subsidiary
    UNION ALL
    SELECT f.duns, f.primary_name, f.parent_duns, f.hierarchy_level
    FROM family_tree_members f
    INNER JOIN hierarchy h ON f.duns = h.parent_duns
)
SELECT * FROM hierarchy ORDER BY hierarchy_level;
```

---

### Company Profile

**What industry does a company operate in?**
```sql
SELECT c.primary_name, ic.type_description, ic.code, ic.description
FROM companies c
JOIN industry_codes ic ON c.duns = ic.duns
WHERE c.duns = '081466849'
ORDER BY ic.priority;
```

**Is a company currently active or delisted?**
```sql
SELECT duns, primary_name, operating_status, is_marketable
FROM companies
WHERE duns = '103832861';
```

**Is a company publicly traded or privately owned?**
```sql
SELECT duns, primary_name, control_ownership_type
FROM companies
WHERE duns = '199882911';
```

**Where is a company headquartered?**
```sql
SELECT c.primary_name, a.street_line1, a.city, a.region, a.country
FROM companies c
JOIN addresses a ON c.duns = a.duns
WHERE c.duns = '081466849'
  AND a.address_type = 'primary';
```

---

### Financial & Size

**What is the latest annual revenue for a company?**
```sql
SELECT c.primary_name, f.yearly_revenue, f.currency, f.statement_date
FROM companies c
JOIN financials f ON c.duns = f.duns
WHERE c.duns = '103832861'
ORDER BY f.statement_date DESC
LIMIT 1;
```

**How many employees does a company have at HQ vs. across the whole group?**
```sql
SELECT c.primary_name, e.information_scope, e.employee_count
FROM companies c
JOIN employees e ON c.duns = e.duns
WHERE c.duns = '199882911';
```

**What is a company's net worth and total assets?**
```sql
SELECT c.primary_name, f.total_assets, f.net_worth, f.total_liabilities, f.statement_date
FROM companies c
JOIN financials f ON c.duns = f.duns
WHERE c.duns = '103832861'
ORDER BY f.statement_date DESC
LIMIT 1;
```

---

### Group-Level Analysis

**How many entities belong to a given corporate group?**
```sql
SELECT global_ultimate_duns, COUNT(*) AS total_members
FROM family_tree_members
WHERE global_ultimate_duns = '081466849'
GROUP BY global_ultimate_duns;
```

**Which subsidiaries are in a specific country or region?**
```sql
SELECT duns, primary_name, country, region, hierarchy_level
FROM family_tree_members
WHERE global_ultimate_duns = '199882911'
  AND country = 'United Kingdom'
ORDER BY hierarchy_level, primary_name;
```

**What is the total consolidated revenue across a corporate family?**
```sql
SELECT
    ftm.global_ultimate_duns,
    c.primary_name AS group_name,
    SUM(f.yearly_revenue) AS total_group_revenue,
    f.currency
FROM family_tree_members ftm
JOIN financials f ON ftm.duns = f.duns
JOIN companies c ON ftm.global_ultimate_duns = c.duns
WHERE ftm.global_ultimate_duns = '199882911'
GROUP BY ftm.global_ultimate_duns, c.primary_name, f.currency;
```

---

## Schema Design Decisions

### Why separate `addresses` from `companies`?
A single company can have up to three distinct addresses (primary, registered, mailing). Storing all three as columns in `companies` would create 30+ address columns with a lot of nulls. Normalising into a separate table with an `address_type` column is cleaner and easier to query.

### Why a self-referencing foreign key in `family_tree_members`?
The corporate hierarchy can be arbitrarily deep — Bain Capital's tree, for example, reaches 11 levels. A self-referencing `parent_duns → duns` relationship handles any depth without schema changes. Traversing the full hierarchy requires a recursive SQL query (`WITH RECURSIVE`).

### Why separate `industry_codes` from `companies`?
Each company has between 6 and 10 industry codes across different classification systems (NAICS 2022, D&B SIC, NACE Rev.2, ISIC Rev.4, etc.). These cannot be stored as a flat column — they need their own table with one row per code.

### Why separate `tree_roles` from `family_tree_members`?
A single entity can simultaneously hold multiple roles — for example, a mid-tier holding company can be a "Subsidiary" of the root while also being a "Parent/Headquarters" to its own children. Storing roles as a separate table avoids multi-value columns.

---

## Known Limitations

### `family_tree_members` ↔ `companies` — Partial Coverage
The `companies` table only contains full profiles for the **root companies** for which a `data_blocks.json` file was provided (in this case: Microsoft, Harford Bank, Bain Capital). The `family_tree_members` table can contain hundreds or thousands of subsidiary entities that have **no corresponding row in `companies`** — only the basic identity and hierarchy data available from the family tree is stored for them.

This means the foreign key from `family_tree_members` to `companies` is **optional (nullable)**. A LEFT JOIN must be used when combining the two tables to avoid losing tree members that lack a full company profile.

### Paginated Family Tree Data
The D&B API returns family tree members in pages of up to 1,000 records. For large groups, the total member count may exceed what is loaded. For example, Microsoft's group has 1,299 total members but only 500 are present in the provided file (branches excluded). The pipeline processes whatever is available in the file but does not fetch additional pages.

### Nullable Fields
Many fields in `data_blocks.json` are null for certain company types. Private companies lack stock exchange data; partnerships lack incorporation dates; small companies lack detailed financials. All non-identity fields in the schema are nullable to reflect this. Nulls represent missing data at source, not pipeline errors.

### Financial Data Staleness
Financial records are point-in-time snapshots as provided by D&B. The `statement_date` field indicates the reporting period. No historical time series is guaranteed — some companies have only one financial record, others may have none.

---

## Scaling Considerations

> The current implementation is designed for the provided sample dataset. For production use with significantly larger inputs, the following changes are recommended — **without modifying the underlying infrastructure**:

### 1. Stream JSON parsing instead of loading into memory
The current pipeline uses `json.load()` which reads the entire file into memory at once. For very large `family_tree.json` files (millions of members), this will exhaust available RAM. Replacing it with **`ijson`** (incremental JSON parser) allows the file to be processed record by record in a streaming fashion, keeping memory usage constant regardless of file size.

```python
# Instead of: data = json.load(f)
import ijson
with open('family_tree.json', 'rb') as f:
    for member in ijson.items(f, 'familyTreeMembers.item'):
        process(member)
```

### 2. Process in chunks using pandas or Polars
Instead of building one large DataFrame in memory, the pipeline can accumulate records in fixed-size batches and write each batch to Parquet incrementally using **append mode**. This keeps peak memory low and allows the pipeline to resume from a checkpoint if interrupted.

---

## Output

The pipeline produces a single enriched Parquet file (`output/enriched_companies.parquet`) containing one row per family tree member, with the `parent_duns` and `global_ultimate_duns` attached from the hierarchy data, and the full company profile joined where available.

---

## Project Structure

```
.
├── data/
│   ├── company_a/
│   │   ├── data_blocks.json
│   │   └── family_tree.json
│   ├── company_b/
│   │   ├── data_blocks.json
│   │   └── family_tree.json
│   └── company_c/
│       ├── data_blocks.json
│       └── family_tree.json
├── output/
│   └── enriched_companies.parquet
├── pipeline.py
├── test_pipeline.py
├── schema.dbml
└── README.md
```

---

## Dependencies

```
pandas
pyarrow
pytest
ijson        # for streaming JSON (scaling)
```
