# Bug Fixes Applied

**Date:** 2026-01-28

---

## Bugs Fixed

### 1. ✅ Virtual Environment Error
**Issue:** `error: externally-managed-environment` when trying to install packages

**Fix:**
- Created `.venv` virtual environment (already exists)
- Added `setup_env.sh` script for easy activation

**How to use:**
```bash
source .venv/bin/activate  # Activate venv
pip install -r requirements.txt  # Install dependencies
```

---

### 2. ✅ DATABASE_URL Parsing Error
**Issue:** `invalid literal for int() with base 10: 'port'`

**Root cause:** `.env` file had placeholder `postgresql://user:password@host:port/maryland_atlas`

**Fix:** Updated `.env` with correct format:
```bash
DATABASE_URL=postgresql://localhost/maryland_atlas
```

---

### 3. ✅ Junk in .env File
**Issue:** PostgreSQL installation notes accidentally pasted at end of `.env`

**Fix:** Removed lines 27-49 (installation notes)

---

### 4. ✅ SQL Schema Parsing Error
**Issue:** `unterminated dollar-quoted string` when executing schema.sql

**Root cause:** `init_db()` function was splitting SQL by semicolons, which broke PostgreSQL function definitions using `$` delimiters (like `calculate_entropy`)

**Fix:** Modified `config/database.py` to use subprocess + psql command directly instead of splitting SQL
```python
result = subprocess.run(
    [psql_cmd, db_url, '-f', schema_path],
    capture_output=True,
    text=True
)
```

---

### 5. ✅ psql Command Not in PATH
**Issue:** `init_db()` function couldn't find `psql` command on macOS

**Root cause:** PostgreSQL installed via Homebrew, but bin directory not in PATH

**Fix:** Updated `config/database.py` to check common Homebrew paths on macOS:
```python
if platform.system() == 'Darwin':  # macOS
    homebrew_paths = [
        '/opt/homebrew/opt/postgresql@17/bin/psql',
        '/opt/homebrew/opt/postgresql@16/bin/psql',
        ...
    ]
```

---

### 6. ✅ Wrong Virtual Environment (Python 3.14 Incompatibility)
**Issue:** `pandas` build failed with Python 3.14 compatibility error

**Root cause:** Accidentally created virtual environment at `path/to/venv` with Python 3.14 instead of using `.venv` with Python 3.9

**Error:**
```
error: too few arguments to function call, expected 6, have 5
_PyLong_AsByteArray
```

**Fix:** Removed incorrect `path/to/venv` directory. Always use `.venv`:
```bash
source .venv/bin/activate  # Correct
python --version  # Should show Python 3.9.6
```

---

### 7. ✅ NaN Values Causing Database Insert Errors
**Issue:** `integer out of range` error when inserting employment data

**Root cause:** Pandas NaN values can't be inserted into PostgreSQL INTEGER columns

**Error:**
```
psycopg2.errors.NumericValueOutOfRange: integer out of range
'avg_weekly_wage': nan, 'qcew_total_establishments': nan
```

**Fix:** Modified `src/ingest/layer1_employment.py:430-432` to convert NaN to None:
```python
# Convert row to dict and replace NaN with None for PostgreSQL NULL
row_dict = row.to_dict()
row_dict = {k: (None if pd.isna(v) else v) for k, v in row_dict.items()}
db.execute(sql, row_dict)
```

---

## Setup Instructions

### Step 1: Activate Virtual Environment
```bash
cd "/Users/elliotkarikari/Dev Projects/Maryland Housing"
source .venv/bin/activate
```

Your prompt should show `(.venv)` when activated.

### Step 2: Install Dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

**Or use the setup script:**
```bash
source setup_env.sh
```

### Step 3: Set Up PostgreSQL
```bash
./setup_database.sh
```

This will:
- Start PostgreSQL@17
- Create `maryland_atlas` database
- Enable PostGIS extension
- Test connection

**Manual alternative:**
```bash
# Start PostgreSQL
brew services start postgresql@17

# Create database
createdb maryland_atlas

# Enable PostGIS
psql maryland_atlas -c "CREATE EXTENSION IF NOT EXISTS postgis;"
```

### Step 4: Initialize Database Schema
```bash
python scripts/init_db.py
```

Expected output:
```
✓ Database connection successful
✓ Schema initialized
✓ County boundaries loaded
✓ Verification complete
```

### Step 5: Test Imports
```bash
python -c "
from config.settings import get_settings
from config.database import test_connection
print('Settings:', get_settings())
print('DB Connection:', test_connection())
"
```

---

## Verification Checklist

Run these commands to verify everything is working:

```bash
# 1. Check virtual environment
which python
# Should show: /Users/elliotkarikari/Dev Projects/Maryland Housing/.venv/bin/python

# 2. Check Python packages
pip list | grep -E "sqlalchemy|pydantic|openai"
# Should show: sqlalchemy, pydantic, openai, etc.

# 3. Check PostgreSQL
psql maryland_atlas -c "SELECT version();"
# Should show: PostgreSQL 17.x

# 4. Check PostGIS
psql maryland_atlas -c "SELECT PostGIS_version();"
# Should show: PostGIS version

# 5. Test database connection
python -c "from config.database import test_connection; print(test_connection())"
# Should show: True
```

---

## Common Issues & Solutions

### Issue: "psql: command not found"

**Solution:** Add PostgreSQL to PATH:
```bash
echo 'export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### Issue: "Could not connect to database"

**Solution:**
```bash
# Check if PostgreSQL is running
brew services list | grep postgresql

# Restart if needed
brew services restart postgresql@17
```

### Issue: "CREATE EXTENSION IF NOT EXISTS postgis"

**Solution:** Install PostGIS:
```bash
brew install postgis
```

### Issue: "ImportError: No module named 'openai'"

**Solution:** Activate virtual environment first:
```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### Issue: "CENSUS_API_KEY not set"

**Solution:** Get free key at https://api.census.gov/data/key_signup.html
Then update `.env`:
```bash
CENSUS_API_KEY=your_actual_key_here
```

---

## What's Working Now

After these fixes, you should be able to:

✅ Activate virtual environment
✅ Import all Python modules
✅ Connect to PostgreSQL database
✅ Run database initialization
✅ Execute the full pipeline

---

## Next Steps

1. **Test the pipeline:**
```bash
source .venv/bin/activate
python src/run_pipeline.py --help
```

2. **Run Layer 1 ingestion (test):**
```bash
python -m src.ingest.layer1_employment
```

3. **Start API server:**
```bash
make serve
# Or: uvicorn src.api.main:app --reload
```

4. **View API docs:**
```
http://localhost:8000/docs
```

---

## Files Modified

| File | Changes |
|------|---------|
| `.env` | Fixed DATABASE_URL, removed junk |
| `setup_env.sh` | Created (NEW) |
| `setup_database.sh` | Created (NEW) |
| `BUGFIXES.md` | Created (this file) |

---

## Testing Commands

```bash
# Full test suite
pytest tests/ -v

# Test specific module
pytest tests/test_classification.py -v

# Test with coverage
pytest tests/ --cov=src --cov-report=html
```

---

**All bugs fixed! ✅**

You're ready to start building the remaining layers.
