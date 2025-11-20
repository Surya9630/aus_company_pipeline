# Requirements Files

This project uses **separate requirement files** for better dependency management:

## `requirements-app.txt` - Main Pipeline Dependencies

Install these for running the extraction and matching pipeline:

```bash
pip install -r requirements-app.txt
```

**Includes:**
- Data extraction: `warcio`, `beautifulsoup4`, `lxml`, `requests`
- Database: `sqlalchemy`, `psycopg2-binary`, `pandas`
- Entity matching: `google-generativeai` (Gemini API)
- Utilities: `python-dotenv`, `pyyaml`, `tqdm`

## `requirements-dbt.txt` - dbt Transformations

Install these separately in a dbt-specific virtual environment:

```bash
python -m venv dbt_venv
source dbt_venv/bin/activate
pip install -r requirements-dbt.txt
```

**Includes:**
- `dbt-core`
- `dbt-postgres`

## Why Separate Files?

**Reason 1: Version Conflicts**
- dbt requires specific dependency versions that conflict with the app dependencies
- Separating them prevents dependency hell

**Reason 2: Optional Installation**
- You can run just extraction without dbt
- Or run just dbt without re-installing extraction tools

**Reason 3: Different Environments**
- Production might only need app dependencies
- Analytics team might only need dbt

## Installation Quick Start

### Option 1: Install Everything (Development)

```bash
# Main app
pip install -r requirements-app.txt

# dbt (in separate venv recommended)
python -m venv dbt_venv
source dbt_venv/bin/activate
pip install -r requirements-dbt.txt
```

### Option 2: Install Only What You Need

**Just extraction and matching:**
```bash
pip install -r requirements-app.txt
```

**Just dbt transformations:**
```bash
pip install -r requirements-dbt.txt
```
