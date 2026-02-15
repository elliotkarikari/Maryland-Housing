"""
CIP (Capital Improvement Plan) Document Extractor

Fetches and extracts structured facts from Maryland county CIP documents using AI.

Real data sources (V1):
- Montgomery County: https://www.montgomerycountymd.gov/cip/
- Howard County: https://www.howardcountymd.gov/budget
- Anne Arundel County: https://www.aacounty.org/departments/budget/

This module:
1. Fetches CIP PDFs from county websites
2. Extracts text from PDF
3. Uses AI to extract structured facts (CIPExtraction schema)
4. Stores in database with full provenance
5. Links evidence to counties for policy persistence scoring
"""

import hashlib
import io
import os
from datetime import date, datetime
from typing import Dict, List, Optional

import requests
from sqlalchemy import text

from config.database import get_db, log_refresh
from config.settings import MD_COUNTY_FIPS, get_settings
from src.ai.providers.base import AIProviderError
from src.ai.providers.openai_provider import get_openai_provider
from src.ai.schemas.cip_extraction import CIPExtraction, CIPExtractionResponse
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


# Real Maryland County CIP Sources (verified public URLs)
CIP_SOURCES = {
    "24031": {  # Montgomery County
        "name": "Montgomery County",
        "url": "https://www.montgomerycountymd.gov/OMB/Resources/Files/omb/pdfs/FY25-30/pdffiles/CIP_FY25-30_FINAL_WEB.pdf",
        "title": "Montgomery County FY25-30 CIP",
        "published_date": date(2024, 5, 16),
    },
    # Additional counties can be added as URLs are verified
    # "24027": {  # Howard County
    #     "name": "Howard County",
    #     "url": "TBD - needs verification",
    #     "title": "Howard County CIP",
    #     "published_date": None
    # }
}


def calculate_sha256(content: bytes) -> str:
    """
    Calculate SHA256 hash of content.

    Args:
        content: Bytes to hash

    Returns:
        Hex digest of SHA256 hash
    """
    return hashlib.sha256(content).hexdigest()


def fetch_pdf_content(url: str, timeout: int = 120) -> bytes:
    """
    Download PDF from URL.

    Args:
        url: URL to fetch
        timeout: Request timeout in seconds

    Returns:
        PDF content as bytes

    Raises:
        Exception: If download fails
    """
    logger.info(f"Fetching PDF from {url}")

    try:
        response = requests.get(url, timeout=timeout, allow_redirects=True)
        response.raise_for_status()

        # Verify it's actually a PDF
        content_type = response.headers.get("content-type", "")
        if "pdf" not in content_type.lower() and not url.endswith(".pdf"):
            logger.warning(f"Content type is {content_type}, may not be PDF")

        logger.info(f"Downloaded {len(response.content)} bytes")

        return response.content

    except Exception as e:
        logger.error(f"Failed to fetch PDF: {e}")
        raise


def extract_text_from_pdf(pdf_content: bytes) -> str:
    """
    Extract text from PDF content.

    Uses PyPDF2 for text extraction. Falls back to simpler methods if needed.

    Args:
        pdf_content: PDF file content as bytes

    Returns:
        Extracted text

    Raises:
        Exception: If extraction fails
    """
    logger.info("Extracting text from PDF")

    try:
        from PyPDF2 import PdfReader

        reader = PdfReader(io.BytesIO(pdf_content))

        text_parts = []
        for page_num, page in enumerate(reader.pages):
            text = page.extract_text()
            text_parts.append(text)

            if (page_num + 1) % 10 == 0:
                logger.debug(f"Extracted {page_num + 1} pages")

        full_text = "\n\n".join(text_parts)

        logger.info(f"Extracted {len(full_text)} characters from {len(reader.pages)} pages")

        return full_text

    except ImportError:
        raise Exception("PyPDF2 not installed. Run: pip install PyPDF2")
    except Exception as e:
        logger.error(f"PDF text extraction failed: {e}")
        raise


def store_document_metadata(
    source_url: str,
    title: str,
    publisher: str,
    published_date: Optional[date],
    sha256: str,
    pdf_content: bytes,
    local_path: Optional[str] = None,
) -> int:
    """
    Store document metadata in ai_document table.

    Args:
        source_url: Public URL where document was fetched
        title: Document title
        publisher: Publishing agency/county
        published_date: Official publication date
        sha256: Document hash
        pdf_content: PDF content (for file size)
        local_path: Optional local storage path

    Returns:
        Document ID (primary key)
    """
    logger.info(f"Storing document metadata for {title}")

    with get_db() as db:
        # Check if document already exists (by SHA256)
        check_sql = text("SELECT id FROM ai_document WHERE sha256 = :sha256")
        existing = db.execute(check_sql, {"sha256": sha256}).fetchone()

        if existing:
            logger.info(f"Document already exists with ID {existing[0]} (SHA256 match)")
            return existing[0]

        # Insert new document
        insert_sql = text(
            """
            INSERT INTO ai_document (
                source_url, title, publisher, published_date,
                sha256, local_path, file_size_bytes, mime_type, fetched_at
            ) VALUES (
                :source_url, :title, :publisher, :published_date,
                :sha256, :local_path, :file_size_bytes, :mime_type, :fetched_at
            )
            RETURNING id
        """
        )

        result = db.execute(
            insert_sql,
            {
                "source_url": source_url,
                "title": title,
                "publisher": publisher,
                "published_date": published_date,
                "sha256": sha256,
                "local_path": local_path,
                "file_size_bytes": len(pdf_content),
                "mime_type": "application/pdf",
                "fetched_at": datetime.utcnow(),
            },
        )

        doc_id = result.fetchone()[0]
        db.commit()

        logger.info(f"Stored document with ID {doc_id}")

        return doc_id


def store_extraction_result(
    doc_id: int, task_name: str, extraction_response: Dict, prompt_version: str
) -> int:
    """
    Store AI extraction result in ai_extraction table.

    Args:
        doc_id: Document ID (foreign key)
        task_name: Extraction task name
        extraction_response: Response dict from AI provider
        prompt_version: Prompt version used

    Returns:
        Extraction ID (primary key)
    """
    logger.info(f"Storing extraction result for doc_id={doc_id}, task={task_name}")

    with get_db() as db:
        # Check for existing extraction
        check_sql = text(
            """
            SELECT id FROM ai_extraction
            WHERE doc_id = :doc_id AND task_name = :task_name AND prompt_version = :prompt_version
        """
        )

        existing = db.execute(
            check_sql, {"doc_id": doc_id, "task_name": task_name, "prompt_version": prompt_version}
        ).fetchone()

        if existing:
            logger.info(f"Extraction already exists with ID {existing[0]}")
            return existing[0]

        # Convert extracted_facts to dict for JSONB storage
        extracted_facts = extraction_response.get("extracted_facts")
        if extracted_facts and hasattr(extracted_facts, "model_dump"):
            extracted_facts_json = extracted_facts.model_dump()
        else:
            extracted_facts_json = None

        # Insert extraction
        insert_sql = text(
            """
            INSERT INTO ai_extraction (
                doc_id, task_name, model, prompt_version,
                output_json, extracted_facts_json, confidence,
                tokens_input, tokens_output, cost_estimate,
                validation_status, error_message
            ) VALUES (
                :doc_id, :task_name, :model, :prompt_version,
                :output_json, :extracted_facts_json, :confidence,
                :tokens_input, :tokens_output, :cost_estimate,
                :validation_status, :error_message
            )
            RETURNING id
        """
        )

        result = db.execute(
            insert_sql,
            {
                "doc_id": doc_id,
                "task_name": task_name,
                "model": extraction_response["model"],
                "prompt_version": prompt_version,
                "output_json": extraction_response.get("raw_output"),
                "extracted_facts_json": extracted_facts_json,
                "confidence": extraction_response.get("confidence"),
                "tokens_input": extraction_response["tokens_input"],
                "tokens_output": extraction_response["tokens_output"],
                "cost_estimate": extraction_response["cost_estimate"],
                "validation_status": extraction_response["validation_status"],
                "error_message": extraction_response.get("error_message"),
            },
        )

        extraction_id = result.fetchone()[0]
        db.commit()

        logger.info(f"Stored extraction with ID {extraction_id}")

        return extraction_id


def link_evidence_to_county(
    fips_code: str, doc_id: int, extraction_id: int, extraction: CIPExtraction
):
    """
    Create evidence links from extraction to county.

    Args:
        fips_code: County FIPS code
        doc_id: Document ID
        extraction_id: Extraction ID
        extraction: Validated CIPExtraction object
    """
    logger.info(f"Linking evidence to county {fips_code}")

    # Convert extraction to evidence claims
    claims = extraction.to_evidence_claims(fips_code)

    if not claims:
        logger.warning("No claims to link")
        return

    with get_db() as db:
        for claim in claims:
            insert_sql = text(
                """
                INSERT INTO ai_evidence_link (
                    geoid, doc_id, extraction_id,
                    claim_type, claim_value, claim_value_unit,
                    claim_date, weight
                ) VALUES (
                    :geoid, :doc_id, :extraction_id,
                    :claim_type, :claim_value, :claim_value_unit,
                    :claim_date, :weight
                )
                ON CONFLICT DO NOTHING
            """
            )

            db.execute(
                insert_sql,
                {
                    "geoid": fips_code,
                    "doc_id": doc_id,
                    "extraction_id": extraction_id,
                    "claim_type": claim["claim_type"],
                    "claim_value": claim["claim_value"],
                    "claim_value_unit": claim["claim_value_unit"],
                    "claim_date": claim.get("claim_date"),
                    "weight": claim.get("weight", 1.0),
                },
            )

        db.commit()

    logger.info(f"Linked {len(claims)} evidence claims")


def extract_cip_for_county(fips_code: str, force_refresh: bool = False) -> Optional[Dict]:
    """
    Extract CIP data for a single county.

    Args:
        fips_code: County FIPS code (e.g., '24031')
        force_refresh: If True, re-extract even if cached

    Returns:
        Dict with extraction results or None if not available

    Raises:
        AIProviderError: If extraction fails unrecoverably
    """
    if fips_code not in CIP_SOURCES:
        logger.warning(f"No CIP source configured for {fips_code}")
        return None

    source = CIP_SOURCES[fips_code]

    logger.info(f"Extracting CIP for {source['name']} ({fips_code})")

    try:
        # Fetch PDF
        pdf_content = fetch_pdf_content(source["url"])

        # Calculate hash
        sha256 = calculate_sha256(pdf_content)

        # Check cache (unless force refresh)
        if not force_refresh:
            with get_db() as db:
                cache_check = text(
                    """
                    SELECT ae.id, ae.extracted_facts_json
                    FROM ai_document ad
                    JOIN ai_extraction ae ON ad.id = ae.doc_id
                    WHERE ad.sha256 = :sha256
                        AND ae.task_name = 'cip_capital_commitment'
                        AND ae.validation_status = 'valid'
                    ORDER BY ae.created_at DESC
                    LIMIT 1
                """
                )

                cached = db.execute(cache_check, {"sha256": sha256}).fetchone()

                if cached:
                    logger.info(f"Cache hit! Using existing extraction ID {cached[0]}")
                    return {
                        "fips_code": fips_code,
                        "extraction_id": cached[0],
                        "cached": True,
                        "extraction": cached[1],
                    }

        # Store document metadata
        doc_id = store_document_metadata(
            source_url=source["url"],
            title=source["title"],
            publisher=source["name"],
            published_date=source.get("published_date"),
            sha256=sha256,
            pdf_content=pdf_content,
        )

        # Extract text
        text = extract_text_from_pdf(pdf_content)

        # Get AI provider
        provider = get_openai_provider()

        # Extract structured data
        prompt_version = "cip_v1.0"

        extraction_response = provider.extract_structured(
            document_text=text,
            task_name="cip_capital_commitment",
            schema=CIPExtraction,
            prompt_version=prompt_version,
        )

        # Store extraction
        extraction_id = store_extraction_result(
            doc_id=doc_id,
            task_name="cip_capital_commitment",
            extraction_response=extraction_response,
            prompt_version=prompt_version,
        )

        # Link evidence if extraction was valid
        if extraction_response["validation_status"] == "valid":
            extraction = extraction_response["extracted_facts"]
            link_evidence_to_county(fips_code, doc_id, extraction_id, extraction)

        logger.info(
            f"CIP extraction complete for {fips_code}: "
            f"status={extraction_response['validation_status']}, "
            f"cost=${extraction_response['cost_estimate']:.4f}"
        )

        return {
            "fips_code": fips_code,
            "doc_id": doc_id,
            "extraction_id": extraction_id,
            "cached": False,
            "validation_status": extraction_response["validation_status"],
            "cost": extraction_response["cost_estimate"],
            "extraction": extraction_response.get("extracted_facts"),
        }

    except Exception as e:
        logger.error(f"CIP extraction failed for {fips_code}: {e}", exc_info=True)

        # Log failure
        log_refresh(
            layer_name="ai_cip_extraction",
            data_source=f"CIP - {source['name']}",
            status="failed",
            error_message=str(e),
        )

        return None


def run_cip_extraction_all(force_refresh: bool = False, cost_limit: float = 5.0) -> Dict:
    """
    Run CIP extraction for all available counties.

    Args:
        force_refresh: If True, re-extract even if cached
        cost_limit: Maximum total cost in USD

    Returns:
        Summary dict with results
    """
    logger.info(f"Running CIP extraction for all counties (cost_limit=${cost_limit})")

    results = []
    total_cost = 0.0

    for fips_code in CIP_SOURCES.keys():
        # Check cost limit
        if total_cost >= cost_limit:
            logger.warning(f"Cost limit ${cost_limit} reached, stopping")
            break

        result = extract_cip_for_county(fips_code, force_refresh=force_refresh)

        if result:
            results.append(result)

            if not result.get("cached"):
                total_cost += result.get("cost", 0.0)

    # Log summary
    success_count = len([r for r in results if r.get("validation_status") == "valid"])

    log_refresh(
        layer_name="ai_cip_extraction",
        data_source="All county CIPs",
        status="success" if success_count > 0 else "partial",
        records_processed=len(results),
        records_inserted=success_count,
        metadata={
            "total_cost_usd": total_cost,
            "cached_count": len([r for r in results if r.get("cached")]),
            "force_refresh": force_refresh,
        },
    )

    logger.info(
        f"CIP extraction complete: {success_count}/{len(results)} valid, "
        f"total cost ${total_cost:.4f}"
    )

    return {
        "total_counties": len(CIP_SOURCES),
        "processed": len(results),
        "valid": success_count,
        "total_cost": total_cost,
        "results": results,
    }


if __name__ == "__main__":
    import argparse

    from src.utils.logging import setup_logging

    setup_logging("cip_extractor")

    parser = argparse.ArgumentParser(description="Extract CIP data from Maryland counties")
    parser.add_argument(
        "--county-fips", type=str, help="Extract for specific county FIPS code (e.g., 24031)"
    )
    parser.add_argument("--all", action="store_true", help="Extract for all available counties")
    parser.add_argument(
        "--force-refresh", action="store_true", help="Force re-extraction even if cached"
    )
    parser.add_argument(
        "--cost-limit", type=float, default=5.0, help="Maximum total cost in USD (default: 5.0)"
    )

    args = parser.parse_args()

    if args.county_fips:
        result = extract_cip_for_county(args.county_fips, force_refresh=args.force_refresh)
        print(f"\nResult: {result}")

    elif args.all:
        summary = run_cip_extraction_all(
            force_refresh=args.force_refresh, cost_limit=args.cost_limit
        )
        print(f"\nSummary: {summary}")

    else:
        parser.print_help()
