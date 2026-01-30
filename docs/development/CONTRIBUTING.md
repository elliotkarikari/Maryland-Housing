# Contributing to Maryland Housing Atlas

Thank you for your interest in contributing to the Maryland Growth & Family Viability Atlas!

## Development Setup

### Prerequisites

- Python 3.10+
- PostgreSQL 14+
- Git
- Census API key (free at [census.gov](https://api.census.gov/data/key_signup.html))
- Mapbox token (free tier at [mapbox.com](https://mapbox.com))

### Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/your-org/maryland-housing.git
cd maryland-housing

# 2. Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your API keys

# 5. Initialize database
make init-db

# 6. Run the API
make run-api

# 7. Serve the frontend (in another terminal)
python frontend/serve.py
```

## Code Style

### Python

- Follow [PEP 8](https://pep8.org/) style guidelines
- Use type hints for function parameters and return values
- Maximum line length: 100 characters
- Use docstrings for public functions

```python
def compute_accessibility_score(
    jobs_df: pd.DataFrame,
    distance_km: float = 45.0
) -> float:
    """
    Compute job accessibility score for a given distance threshold.

    Args:
        jobs_df: DataFrame with job locations and wages
        distance_km: Maximum travel distance in kilometers

    Returns:
        Normalized accessibility score (0-1)
    """
    ...
```

### SQL

- Use lowercase for keywords (`select`, `from`, `where`)
- Use snake_case for table and column names
- Include comments for complex queries

### JavaScript

- Use ES6+ syntax
- Prefer `const` over `let`
- Use template literals for string interpolation

## Project Structure

```
src/
├── api/           # FastAPI endpoints
├── ingest/        # Data ingestion pipelines (one per layer)
├── processing/    # Scoring and classification logic
├── export/        # GeoJSON generation
└── utils/         # Shared utilities
```

### Layer Development Pattern

Each layer follows a consistent pattern:

1. **Ingestion Script**: `src/ingest/layerN_*.py`
   - Fetches data from external sources
   - Computes raw metrics
   - Stores in PostgreSQL

2. **Migration**: `migrations/0XX_layerN_*.sql`
   - Creates/modifies database schema
   - Includes indexes and constraints

3. **Documentation**: `docs/layers/LAYERN_*.md`
   - Explains methodology
   - Documents data sources
   - Lists all computed metrics

## Pull Request Process

1. **Create a feature branch** from `main`
   ```bash
   git checkout -b feature/add-transit-metrics
   ```

2. **Make your changes**
   - Write clear, focused commits
   - Include tests for new functionality
   - Update documentation as needed

3. **Run quality checks**
   ```bash
   make lint   # Run linters
   make test   # Run tests
   ```

4. **Submit PR**
   - Use a clear, descriptive title
   - Reference any related issues
   - Describe what changes were made and why

5. **Address review feedback**
   - Respond to all comments
   - Make requested changes in new commits

## Commit Messages

Use clear, imperative commit messages:

```
Good:
- Add transit accessibility score calculation
- Fix population-weighted aggregation bug
- Update Layer 2 documentation

Bad:
- fixed stuff
- WIP
- updates
```

## Testing

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_classification.py

# Run with coverage
pytest --cov=src
```

### Writing Tests

- Place tests in the `tests/` directory
- Use descriptive test names
- Include both positive and negative cases

```python
def test_accessibility_score_returns_normalized_value():
    """Accessibility scores should be between 0 and 1."""
    score = compute_accessibility_score(sample_data)
    assert 0 <= score <= 1
```

## Data Quality

When adding or modifying data pipelines:

1. **Validate sources** - Use official government data when possible
2. **Handle missing data** - Use appropriate fallbacks or flags
3. **Document assumptions** - Explain any transformations or imputations
4. **Log anomalies** - Record unexpected values for debugging

## Questions?

- Open an issue for bugs or feature requests
- Check existing documentation in `docs/`
- Review related code for patterns and conventions
