# merge_biblios.pl

Batch merge duplicate bibliographic records in Koha.

**For Koha 24.05+** — uses the `Koha::Biblio->merge_with()` method.

## Features

- Merges items, holds, acquisition orders, serial subscriptions, and more
- Dry-run mode by default (safe preview)
- Configurable MARC framework handling
- Action log attribution via `--user`
- Detailed logging

## Installation

Copy `merge_biblios.pl` to your Koha server (e.g., `/usr/local/bin/` or Koha's `misc/` directory).

```bash
chmod +x merge_biblios.pl
```

## Usage

```bash
# Preview (default - no changes made)
perl merge_biblios.pl -f duplicates.csv --verbose

# Actually perform the merge
perl merge_biblios.pl -f duplicates.csv --commit --verbose

# With logging and user attribution
perl merge_biblios.pl -f duplicates.csv --commit --user 1 --log merge.log --verbose

# Force default MARC framework
perl merge_biblios.pl -f duplicates.csv --commit --default-framework
```

### Options

| Option | Description |
|--------|-------------|
| `--file, -f` | Input file with merge groups (required) |
| `--commit, -c` | Actually perform the merge (default: dry-run) |
| `--user, -u` | Borrowernumber for action_logs attribution |
| `--framework` | MARC framework code to use for merged records |
| `--default-framework` | Force default framework (even if master has another) |
| `--verbose, -v` | Show detailed progress |
| `--log, -l` | Write log to file |
| `--delimiter, -d` | Field delimiter (default: comma) |
| `--help, -h` | Show help |

## Input File Format

One merge group per line. First biblionumber is the **master** (kept), remaining are **children** (merged and deleted).

```
75,801,802,803,804
105,1494,1495,1496
45,900,1591,1592
```

## Finding duplicates

Use this SQL query to identify potential duplicates based on ISBN, title, statement of responsibility, publisher, publication year, and edition:

```sql
SELECT 
    GROUP_CONCAT(DISTINCT bm.biblionumber ORDER BY bm.biblionumber) AS biblionumbers,
    COUNT(DISTINCT bm.biblionumber) AS duplicate_count,
    COUNT(i.itemnumber) AS total_copies,
    TRIM(ExtractValue(bm.metadata, '//datafield[@tag="020"]/subfield[@code="a"][1]')) AS isbn,
    CONCAT_WS(' ', 
        TRIM(ExtractValue(bm.metadata, '//datafield[@tag="245"]/subfield[@code="a"]')),
        TRIM(ExtractValue(bm.metadata, '//datafield[@tag="245"]/subfield[@code="b"]'))
    ) AS title,
    TRIM(ExtractValue(bm.metadata, '//datafield[@tag="245"]/subfield[@code="c"]')) AS sor,
    TRIM(ExtractValue(bm.metadata, '//datafield[@tag="260"]/subfield[@code="b"]')) AS publisher,
    TRIM(ExtractValue(bm.metadata, '//datafield[@tag="260"]/subfield[@code="c"]')) AS pub_year,
    TRIM(ExtractValue(bm.metadata, '//datafield[@tag="250"]/subfield[@code="a"]')) AS edition
FROM biblio_metadata bm
INNER JOIN items i ON i.biblionumber = bm.biblionumber
GROUP BY 
    TRIM(ExtractValue(bm.metadata, '//datafield[@tag="020"]/subfield[@code="a"][1]')),
    CONCAT_WS(' ', 
        TRIM(ExtractValue(bm.metadata, '//datafield[@tag="245"]/subfield[@code="a"]')),
        TRIM(ExtractValue(bm.metadata, '//datafield[@tag="245"]/subfield[@code="b"]'))
    ),
    TRIM(ExtractValue(bm.metadata, '//datafield[@tag="245"]/subfield[@code="c"]')),
    TRIM(ExtractValue(bm.metadata, '//datafield[@tag="260"]/subfield[@code="b"]')),
    TRIM(ExtractValue(bm.metadata, '//datafield[@tag="260"]/subfield[@code="c"]')),
    TRIM(ExtractValue(bm.metadata, '//datafield[@tag="250"]/subfield[@code="a"]'))
HAVING COUNT(DISTINCT bm.biblionumber) > 1
ORDER BY duplicate_count DESC, title
```

Export the `biblionumbers` column to create the input file for the script.

**Note:** This query only returns biblios that have items attached. Adjust matching criteria as needed for your data.

## What gets merged

The `Koha::Biblio->merge_with()` method handles:

- Items (holdings)
- Holds/Reserves
- Acquisition orders
- Serial subscriptions
- Course reserves
- ILL requests
- Recalls
- Tags

## Requirements

- Koha 24.05 or later
- Run as Koha instance user or via `koha-shell`
- `CataloguingLog` syspref enabled for action logging

## Post-merge

Rebuild your search index (just a fail-safe, it should happen automatically) after merging:

```bash
# Zebra
koha-rebuild-zebra -f -v <instance>

# Elasticsearch
koha-elasticsearch --rebuild -v <instance>
```

## Author

Indranil Das Gupta <indradg@l2c2.co.in> for L2C2 Technologies

## Acknowledgment

Kyle M Hall, ByWater Solutions for the original Record Merger plugin  
https://github.com/bywatersolutions/dev-koha-plugin-record-merger

## License

GPL v3
