# AI Subsystem Architecture

**Maryland Viability Atlas - AI as a First-Class Data Producer**

## Overview

This AI subsystem is **not** a post-hoc layer added to generate narratives or "make the product smarter." It is a **data ingestion pipeline** that extracts structured facts from unstructured policy documents.

### What AI Is Used For

1. **Document Extraction** (Primary Use Case)
   - Extract structured facts from County Capital Improvement Plans (CIPs)
   - Parse budget documents for capital spending commitments
   - Identify multi-year project commitments
   - Detect follow-through on past plans

2. **Future: Narrative Generation** (Secondary, Optional)
   - Generate human-readable explanations from deterministic scores
   - Summarize evidence for classifications
   - **NEVER** used to override or modify scores

### What AI Is NOT Used For

- ❌ Scoring or classification (100% rule-based)
- ❌ Predicting outcomes
- ❌ Replacing transparent logic with black-box decisions
- ❌ Hiding uncertainty behind confidence scores

---

## Design Principles

### 1. Provenance is Mandatory

Every AI extraction must record:
- Source document (URL, hash, fetch date)
- Model used (name, version)
- Prompt version
- Extracted facts (structured JSON)
- Token count and cost estimate
- Timestamp

### 2. Caching is First-Class

- Same document SHA256 = skip re-extraction
- Cache hits logged explicitly
- Cost tracking prevents runaway spending

### 3. Structured Output with Validation

- All extractions validate against Pydantic schemas
- Failed validations logged, not silently dropped
- Confidence/uncertainty explicitly modeled

### 4. Graceful Degradation

- If AI extraction fails → system continues with reduced confidence
- Missing CIP data → lower maximum policy persistence score
- Never block core pipeline on AI availability

### 5. Governance

- Cost limits enforced per run
- Manual review flags for outlier extractions
- Version all prompts (no "prompt drift")

---

## Database Schema

### `ai_document` Table

Stores metadata for all documents processed by AI.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| source_url | TEXT | Public URL where document was fetched |
| title | VARCHAR(500) | Document title |
| publisher | VARCHAR(200) | Publishing agency (e.g., "Montgomery County") |
| published_date | DATE | Official publication date |
| fetched_at | TIMESTAMP | When we downloaded it |
| sha256 | VARCHAR(64) | Document hash (for cache lookup) |
| local_path | TEXT | Path to stored document (if retained) |
| file_size_bytes | INTEGER | File size |
| mime_type | VARCHAR(100) | Document type (e.g., "application/pdf") |
| metadata_json | JSONB | Additional metadata |

### `ai_extraction` Table

Stores AI extraction results.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| doc_id | INTEGER | Foreign key to ai_document |
| task_name | VARCHAR(100) | Extraction task (e.g., "cip_capital_commitment") |
| model | VARCHAR(100) | Model used (e.g., "gpt-5.1-mini") |
| prompt_version | VARCHAR(50) | Prompt version ID |
| output_json | JSONB | Raw model output |
| extracted_facts_json | JSONB | Validated, structured facts |
| confidence | NUMERIC(3,2) | Model-reported confidence (if available) |
| tokens_input | INTEGER | Input token count |
| tokens_output | INTEGER | Output token count |
| cost_estimate | NUMERIC(10,6) | Estimated cost in USD |
| created_at | TIMESTAMP | Extraction timestamp |
| validation_status | VARCHAR(20) | 'valid', 'failed', 'manual_review' |
| error_message | TEXT | Validation error details |

### `ai_evidence_link` Table

Links AI-extracted facts to geographic areas and claims.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| geoid | VARCHAR(5) | FIPS code (e.g., '24031') |
| doc_id | INTEGER | Foreign key to ai_document |
| extraction_id | INTEGER | Foreign key to ai_extraction |
| claim_type | VARCHAR(100) | Type of claim (e.g., "capital_commitment") |
| claim_value | NUMERIC | Quantitative value (e.g., dollars, years) |
| claim_date | DATE | Temporal reference for claim |
| weight | NUMERIC(3,2) | Weight in scoring (0-1) |
| notes | TEXT | Additional context |

---

## Provider Interface

All AI providers must implement:

```python
class AIProvider(ABC):
    @abstractmethod
    def extract_structured(
        self,
        document_text: str,
        task_name: str,
        schema: Type[BaseModel],
        prompt_version: str
    ) -> dict:
        """
        Extract structured data from document.

        Args:
            document_text: Raw text to process
            task_name: Extraction task identifier
            schema: Pydantic model for output validation
            prompt_version: Prompt version ID

        Returns:
            dict with:
                - extracted_facts: validated data
                - confidence: model confidence (if available)
                - metadata: token counts, model info, etc.
        """
        pass
```

### Supported Providers (V1)

- **OpenAI** (primary): GPT-4 Turbo for document extraction
- **Future**: Anthropic Claude, local models

---

## Extraction Tasks

### Task: CIP Capital Commitment

**Purpose:** Extract capital spending signals from County CIPs

**Input:** PDF or text of Capital Improvement Plan

**Output Schema:**
```python
class CIPExtraction(BaseModel):
    total_capital_budget: Optional[float]  # Total $ planned
    school_capital_budget: Optional[float]  # School-specific $
    transport_capital_budget: Optional[float]  # Transport $
    years_covered: List[int]  # Fiscal years in plan
    multi_year_commitments: int  # Count of >1 year projects
    project_count: Optional[int]  # Total projects listed
    delivery_metrics: Optional[dict]  # Past completion rates if stated
    confidence: float  # Extraction confidence (0-1)
```

**Prompt Version:** `cip_v1.0`

**Usage in Scoring:**
- Feeds `policy_persistence.cip_follow_through_rate`
- Contributes to confidence classification
- Missing = lower maximum confidence score (not failure)

---

## Cost Controls

### Per-Run Limits

- Maximum $5.00 per pipeline run (configurable)
- Halt if exceeded, log warning
- Track cumulative spend in database

### Caching Strategy

- SHA256 match = skip extraction
- Refresh only if:
  - Document updated (new hash)
  - Prompt version changed
  - Manual override flag set

---

## Validation & Quality Assurance

### Automatic Validation

1. Schema validation (Pydantic)
2. Range checks (e.g., budgets > 0, years in valid range)
3. Internal consistency (e.g., school budget ≤ total budget)

### Manual Review Triggers

- Extraction confidence < 0.6
- Budget values > 3 std dev from county mean
- Validation warnings (non-fatal)

### Audit Trail

All extractions logged to `ai_extraction` table:
- Successful extractions
- Failed validations
- Cache hits
- Manual overrides

---

## Example: CIP Extraction Flow

```
1. Fetch CIP PDF
   ↓
2. Calculate SHA256 hash
   ↓
3. Check cache (ai_document.sha256)
   ↓
   If cached → load existing extraction
   If new → continue
   ↓
4. Extract text from PDF
   ↓
5. Call OpenAI with structured output
   ↓
6. Validate against CIPExtraction schema
   ↓
7. Store in ai_extraction table
   ↓
8. Link to county in ai_evidence_link
   ↓
9. Use in policy_persistence scoring
```

---

## Environment Variables Required

```bash
# OpenAI
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-5.1-mini  # Default model
OPENAI_MAX_TOKENS=4000

# Cost limits
AI_COST_LIMIT_PER_RUN=5.00  # USD
AI_ENABLED=true  # Master switch
```

---

## Running AI Extraction

### Standalone

```bash
# Extract from a single CIP
python -m src.ai.pipeline.cip_extractor \
    --url https://example.com/county_cip_2025.pdf \
    --county-fips 24031

# Extract for all available counties
python -m src.ai.pipeline.cip_extractor --all
```

### As Part of Pipeline

```bash
# Full pipeline with AI
python src/run_pipeline.py --run-ai true

# Skip AI extraction
python src/run_pipeline.py --run-ai false
```

---

## Limitations & Transparency

### Current Coverage (V1)

| County | CIP Availability | AI Extraction Status |
|--------|-----------------|---------------------|
| Montgomery | ✅ Public PDF | Implemented |
| Howard | ✅ Public PDF | Pending |
| Anne Arundel | ✅ Public PDF | Pending |
| Others | ❌ Not standardized | Future |

**V1 Reality:** Only ~3-5 counties will have AI-extracted CIP data. This is **acceptable** and **documented** rather than fabricated.

### Known AI Limitations

1. **Hallucination risk**: Structured output reduces but doesn't eliminate
2. **PDF quality dependency**: OCR errors propagate
3. **Format variation**: Each county formats CIPs differently
4. **Temporal misalignment**: CIPs updated on different schedules

### Mitigation Strategies

- Conservative confidence thresholds
- Manual review for outliers
- Explicit "data not available" vs "data says X"
- Document which counties have AI-extracted vs manual data

---

## Future Enhancements

### Near-Term

- [ ] Expand to 10+ counties with accessible CIPs
- [ ] Historical CIP analysis (multi-year follow-through)
- [ ] Budget vs actual spend reconciliation

### Long-Term

- [ ] Planning commission meeting minute extraction
- [ ] Zoning board decision pattern analysis
- [ ] Narrative explanation generation (optional frontend feature)

---

## Contact & Governance

**Owner:** Maryland Viability Atlas Team
**Prompt Versioning:** All prompts in `src/ai/prompts/` with semantic versioning
**Cost Monitoring:** Monthly spend reports logged to `ai_extraction` table
**Manual Review Queue:** Extractions flagged for review tracked in database
