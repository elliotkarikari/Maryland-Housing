"""
Pydantic schemas for CIP (Capital Improvement Plan) extraction

These schemas define the structured output format for AI document extraction.
All fields are validated for type, range, and consistency.
"""

from pydantic import BaseModel, Field, validator, field_validator
from typing import Optional, List, Dict
from datetime import date


class CIPProjectItem(BaseModel):
    """Individual capital project extracted from CIP"""
    project_name: str = Field(..., description="Project name or description")
    category: Optional[str] = Field(None, description="Category (e.g., 'schools', 'transportation')")
    total_cost: Optional[float] = Field(None, ge=0, description="Total project cost in USD")
    fy_start: Optional[int] = Field(None, ge=2020, le=2050, description="Fiscal year start")
    fy_end: Optional[int] = Field(None, ge=2020, le=2050, description="Fiscal year end")
    status: Optional[str] = Field(None, description="Project status (planned, approved, funded, in progress)")

    @field_validator('fy_end')
    @classmethod
    def end_after_start(cls, v, info):
        """Validate that end year is after start year"""
        if v is not None and 'fy_start' in info.data and info.data['fy_start'] is not None:
            if v < info.data['fy_start']:
                raise ValueError('End year must be >= start year')
        return v


class DeliveryMetrics(BaseModel):
    """Past delivery performance metrics if stated in CIP"""
    projects_planned_prior_period: Optional[int] = Field(None, ge=0, description="Projects planned in prior CIP")
    projects_completed_prior_period: Optional[int] = Field(None, ge=0, description="Projects actually completed")
    completion_rate: Optional[float] = Field(None, ge=0, le=1, description="Completion rate (0-1)")
    budget_adherence_pct: Optional[float] = Field(None, description="% of projects on budget")

    @field_validator('completion_rate')
    @classmethod
    def validate_completion_rate(cls, v, info):
        """Calculate completion rate if not provided"""
        if v is None and 'projects_planned_prior_period' in info.data and 'projects_completed_prior_period' in info.data:
            planned = info.data['projects_planned_prior_period']
            completed = info.data['projects_completed_prior_period']
            if planned and planned > 0:
                return completed / planned
        return v


class CIPExtraction(BaseModel):
    """
    Structured extraction from County Capital Improvement Plan

    This schema defines what we extract from CIP documents to inform
    policy persistence scoring.
    """

    # Document metadata
    document_title: str = Field(..., description="CIP document title")
    publishing_county: str = Field(..., description="County name")
    publication_date: Optional[date] = Field(None, description="Official publication date")

    # Financial aggregates
    total_capital_budget: Optional[float] = Field(
        None,
        ge=0,
        description="Total capital budget across all categories (USD)"
    )
    school_capital_budget: Optional[float] = Field(
        None,
        ge=0,
        description="Capital budget specifically for schools (USD)"
    )
    transport_capital_budget: Optional[float] = Field(
        None,
        ge=0,
        description="Capital budget for transportation infrastructure (USD)"
    )
    other_capital_budget: Optional[float] = Field(
        None,
        ge=0,
        description="Other capital spending not in above categories (USD)"
    )

    # Temporal scope
    years_covered: List[int] = Field(
        ...,
        description="Fiscal years covered by this CIP (e.g., [2025, 2026, 2027])"
    )

    # Project characteristics
    multi_year_commitments: Optional[int] = Field(
        None,
        ge=0,
        description="Count of projects spanning more than one fiscal year"
    )
    project_count: Optional[int] = Field(
        None,
        ge=0,
        description="Total number of capital projects listed"
    )

    # Detailed projects (optional, for deeper analysis)
    projects: Optional[List[CIPProjectItem]] = Field(
        None,
        max_length=100,  # Limit to prevent token explosion
        description="Individual projects (up to 100 largest)"
    )

    # Past performance
    delivery_metrics: Optional[DeliveryMetrics] = Field(
        None,
        description="Historical delivery performance if stated in document"
    )

    # Extraction metadata
    confidence: float = Field(
        ...,
        ge=0,
        le=1,
        description="Extraction confidence (0-1, from model or validator)"
    )
    extraction_notes: Optional[str] = Field(
        None,
        max_length=1000,
        description="Notes about extraction quality or ambiguities"
    )

    @field_validator('school_capital_budget', 'transport_capital_budget', 'other_capital_budget')
    @classmethod
    def validate_budget_components(cls, v, info):
        """Validate that component budgets don't exceed total"""
        if v is not None and 'total_capital_budget' in info.data:
            total = info.data['total_capital_budget']
            if total is not None and v > total:
                raise ValueError(f'Component budget ({v}) exceeds total budget ({total})')
        return v

    @field_validator('years_covered')
    @classmethod
    def validate_years(cls, v):
        """Validate year range is reasonable"""
        if not v:
            raise ValueError('years_covered cannot be empty')

        if len(v) > 20:
            raise ValueError('CIP covers implausibly many years (>20)')

        if any(year < 2020 or year > 2050 for year in v):
            raise ValueError('Years must be between 2020 and 2050')

        return sorted(v)  # Ensure chronological order

    @field_validator('projects')
    @classmethod
    def validate_projects_count(cls, v, info):
        """Validate project list consistency"""
        if v is not None and 'project_count' in info.data:
            stated_count = info.data['project_count']
            if stated_count is not None and len(v) > stated_count:
                raise ValueError(f'More projects listed ({len(v)}) than stated count ({stated_count})')
        return v

    def calculate_follow_through_rate(self) -> Optional[float]:
        """
        Calculate follow-through rate if delivery metrics available.

        Returns:
            Float 0-1 or None if data not available
        """
        if self.delivery_metrics and self.delivery_metrics.completion_rate is not None:
            return self.delivery_metrics.completion_rate
        return None

    def calculate_school_budget_share(self) -> Optional[float]:
        """
        Calculate share of total budget allocated to schools.

        Returns:
            Float 0-1 or None if data not available
        """
        if self.school_capital_budget is not None and self.total_capital_budget is not None:
            if self.total_capital_budget > 0:
                return self.school_capital_budget / self.total_capital_budget
        return None

    def to_evidence_claims(self, fips_code: str) -> List[Dict]:
        """
        Convert extraction to evidence claims for ai_evidence_link table.

        Args:
            fips_code: County FIPS code

        Returns:
            List of dicts ready for database insertion
        """
        claims = []

        if self.total_capital_budget is not None:
            claims.append({
                'claim_type': 'total_capital_budget',
                'claim_value': self.total_capital_budget,
                'claim_value_unit': 'USD',
                'claim_date': self.publication_date,
                'weight': 1.0
            })

        if self.school_capital_budget is not None:
            claims.append({
                'claim_type': 'school_capital_budget',
                'claim_value': self.school_capital_budget,
                'claim_value_unit': 'USD',
                'claim_date': self.publication_date,
                'weight': 1.5  # Higher weight for school investment
            })

        if self.multi_year_commitments is not None:
            claims.append({
                'claim_type': 'multi_year_commitments',
                'claim_value': self.multi_year_commitments,
                'claim_value_unit': 'count',
                'claim_date': self.publication_date,
                'weight': 1.2  # Multi-year commitments show persistence
            })

        follow_through = self.calculate_follow_through_rate()
        if follow_through is not None:
            claims.append({
                'claim_type': 'cip_follow_through_rate',
                'claim_value': follow_through,
                'claim_value_unit': 'rate',
                'claim_date': self.publication_date,
                'weight': 2.0  # Historical follow-through is highly relevant
            })

        return claims


class CIPExtractionResponse(BaseModel):
    """Response wrapper for CIP extraction including metadata"""
    extraction: CIPExtraction
    model: str
    prompt_version: str
    tokens_input: int
    tokens_output: int
    cost_estimate: float
    validation_status: str = Field(..., pattern='^(valid|failed|manual_review)$')
    error_message: Optional[str] = None
