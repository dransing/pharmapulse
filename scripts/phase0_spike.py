"""
PharmaPulse - Phase 0 Feasibility Spike
=======================================

Answers three questions before any schema or UI is built:

  Q1. Does the XBRL tag ResearchAndDevelopmentExpense exist for our test
      companies, with usable annual 10-K values over ~10 years?
  Q2. What are the REAL ClinicalTrials.gov lead-sponsor name variants for each
      company? (This output becomes the seed data for the sponsor_aliases table.)
  Q3. Does the capital-efficiency metric produce a useful spread across
      companies, or does everyone land in the same narrow band?

Runs in GitHub Actions. Requires no local setup.
Writes a human-readable report to phase0_report.md and prints it to the log.

Environment variables:
  SEC_USER_AGENT  (required)  e.g. "PharmaPulse Research you@example.com"
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import requests


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

# Test universe: 3 large-cap pharma + 2 mid-cap biotech.
# The mid-caps are deliberate - XBRL tag coverage and sponsor naming are
# usually messier for smaller filers, which is exactly what we need to learn.
TEST_COMPANIES: List[Dict[str, str]] = [
    {"ticker": "PFE",  "name": "Pfizer",            "ctgov_query": "Pfizer"},
    {"ticker": "MRK",  "name": "Merck",             "ctgov_query": "Merck Sharp Dohme"},
    {"ticker": "LLY",  "name": "Eli Lilly",         "ctgov_query": "Eli Lilly"},
    {"ticker": "VRTX", "name": "Vertex",            "ctgov_query": "Vertex Pharmaceuticals"},
    {"ticker": "INCY", "name": "Incyte",            "ctgov_query": "Incyte"},
]

# Phase weights from the engineering spec. These are ASSUMPTIONS reflecting
# rough relative trial cost, not measured values.
PHASE_WEIGHTS: Dict[str, float] = {
    "EARLY_PHASE1": 0.5,
    "PHASE1": 1.0,
    "PHASE2": 3.0,
    "PHASE3": 9.0,
    "PHASE4": 2.0,
}

PHASE_RANK = ["EARLY_PHASE1", "PHASE1", "PHASE2", "PHASE3", "PHASE4"]

RD_TAG_PRIMARY = "ResearchAndDevelopmentExpense"
RD_TAG_FALLBACKS = [
    "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost",
    "ResearchAndDevelopmentExpenseSoftwareExcludingAcquiredInProcessCost",
]

DEFAULT_LAG_YEARS = 2
ANALYSIS_START_YEAR = 2014
ANALYSIS_END_YEAR = 2024

SEC_THROTTLE_SECONDS = 0.25    # 4 req/s, well under the documented 10 req/s ceiling
CTGOV_THROTTLE_SECONDS = 1.5   # conservative against ~50 req/min
CTGOV_MAX_PAGES = 12           # safety cap: 12 x 1000 = 12k studies per company
REQUEST_TIMEOUT = 60


# --------------------------------------------------------------------------
# HTTP helpers
# --------------------------------------------------------------------------

class SpikeError(Exception):
    pass


def get_user_agent() -> str:
    ua = os.environ.get("SEC_USER_AGENT", "").strip()
    if not ua or "@" not in ua:
        raise SpikeError(
            "SEC_USER_AGENT is missing or has no email address in it.\n"
            "The SEC rejects requests without a descriptive User-Agent containing "
            "contact info.\n"
            "Set it as a GitHub Actions secret, for example:\n"
            '  PharmaPulse Research yourname@example.com'
        )
    return ua


def http_get_json(
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    throttle: float = 0.25,
    max_attempts: int = 5,
    label: str = "",
) -> Optional[Any]:
    """GET with exponential backoff. Returns parsed JSON, or None on 404."""
    delay = 2.0
    for attempt in range(1, max_attempts + 1):
        time.sleep(throttle)
        try:
            resp = requests.get(
                url, headers=headers, params=params, timeout=REQUEST_TIMEOUT
            )
        except requests.RequestException as exc:
            if attempt == max_attempts:
                raise SpikeError(f"{label}: network failure after {attempt} attempts: {exc}")
            time.sleep(delay)
            delay *= 2
            continue

        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError as exc:
                raise SpikeError(f"{label}: response was not valid JSON: {exc}")

        if resp.status_code == 404:
            return None

        if resp.status_code == 403:
            # SEC blocks on rate-limit violation. Do not retry harder.
            raise SpikeError(
                f"{label}: HTTP 403 from {url}. This usually means the User-Agent "
                "was rejected or the IP is temporarily blocked for exceeding the "
                "rate limit. Wait 10 minutes and re-run."
            )

        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt == max_attempts:
                raise SpikeError(
                    f"{label}: HTTP {resp.status_code} after {attempt} attempts."
                )
            time.sleep(delay)
            delay *= 2
            continue

        raise SpikeError(f"{label}: unexpected HTTP {resp.status_code} from {url}")

    return None


# --------------------------------------------------------------------------
# Q1 - SEC EDGAR XBRL
# --------------------------------------------------------------------------

def load_ticker_to_cik(headers: Dict[str, str]) -> Dict[str, str]:
    """Returns {TICKER: zero-padded 10-digit CIK}."""
    data = http_get_json(
        "https://www.sec.gov/files/company_tickers.json",
        headers=headers,
        throttle=SEC_THROTTLE_SECONDS,
        label="SEC ticker map",
    )
    if not data:
        raise SpikeError("Could not load the SEC ticker map.")

    mapping: Dict[str, str] = {}
    for row in data.values():
        ticker = str(row.get("ticker", "")).upper()
        cik = str(row.get("cik_str", "")).zfill(10)
        if ticker:
            mapping[ticker] = cik
    return mapping


def fetch_company_facts(cik: str, headers: Dict[str, str]) -> Optional[Dict[str, Any]]:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    return http_get_json(
        url, headers=headers, throttle=SEC_THROTTLE_SECONDS, label=f"companyfacts {cik}"
    )


def _parse_iso(d: str) -> Optional[date]:
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def extract_annual_rd(
    facts: Dict[str, Any]
) -> Tuple[Dict[int, Dict[str, Any]], Optional[str], List[str]]:
    """
    Apply every cleaning rule from the spec:
      - 10-K forms only
      - annual duration only (350-380 days)
      - deduplicate by fiscal year, keeping the most recently FILED record
      - fall back to alternative tags if the primary is absent

    Returns (by_fiscal_year, tag_used, notes)
    """
    notes: List[str] = []
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    if not us_gaap:
        return {}, None, ["No us-gaap facts present in this filing."]

    candidate_tags = [RD_TAG_PRIMARY] + RD_TAG_FALLBACKS
    # Last resort: any tag whose name mentions research and development.
    discovered = [
        t for t in us_gaap.keys()
        if "ResearchAndDevelopment" in t and "Asset" not in t and "Liability" not in t
    ]
    for t in discovered:
        if t not in candidate_tags:
            candidate_tags.append(t)

    for tag in candidate_tags:
        node = us_gaap.get(tag)
        if not node:
            continue
        usd_records = node.get("units", {}).get("USD", [])
        if not usd_records:
            continue

        by_year: Dict[int, Dict[str, Any]] = {}
        restatement_count = 0

        for rec in usd_records:
            if rec.get("form") != "10-K":
                continue
            if rec.get("fp") != "FY":
                continue

            start = _parse_iso(rec.get("start", ""))
            end = _parse_iso(rec.get("end", ""))
            if not start or not end:
                continue

            duration = (end - start).days
            if not (350 <= duration <= 380):
                continue

            fy = rec.get("fy")
            if not isinstance(fy, int):
                continue

            filed = _parse_iso(rec.get("filed", "")) or date.min
            existing = by_year.get(fy)
            if existing:
                restatement_count += 1
                if filed <= existing["filed"]:
                    continue

            by_year[fy] = {
                "value": rec.get("val"),
                "period_end": end,
                "filed": filed,
                "accession": rec.get("accn"),
                "form": rec.get("form"),
                "duration_days": duration,
                "is_restated": existing is not None,
            }

        if by_year:
            if tag != RD_TAG_PRIMARY:
                notes.append(
                    f"Primary tag absent or empty. Used fallback tag: {tag}"
                )
            if restatement_count:
                notes.append(
                    f"{restatement_count} duplicate fiscal-year records found "
                    "(restatements). Kept the most recently filed value for each year."
                )
            return by_year, tag, notes

    notes.append(
        "No usable annual 10-K R&D records found under any candidate tag. "
        f"Tags present containing 'ResearchAndDevelopment': {discovered or 'none'}"
    )
    return {}, None, notes


# --------------------------------------------------------------------------
# Q2 - ClinicalTrials.gov v2
# --------------------------------------------------------------------------

def fetch_ctgov_studies(sponsor_query: str) -> List[Dict[str, Any]]:
    """
    Pull all interventional studies matching a sponsor text query.
    Filtering to INDUSTRY class and interventional type happens in Python
    rather than via query syntax, so a syntax change upstream cannot silently
    return zero rows.
    """
    base = "https://clinicaltrials.gov/api/v2/studies"
    headers = {"Accept": "application/json", "User-Agent": get_user_agent()}

    field_paths = ",".join([
        "protocolSection.identificationModule.nctId",
        "protocolSection.sponsorCollaboratorsModule.leadSponsor",
        "protocolSection.designModule.phases",
        "protocolSection.designModule.studyType",
        "protocolSection.designModule.enrollmentInfo",
        "protocolSection.statusModule.overallStatus",
        "protocolSection.statusModule.startDateStruct",
        "protocolSection.statusModule.studyFirstPostDateStruct",
        "protocolSection.conditionsModule.conditions",
    ])

    studies: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    use_fields = True
    pages = 0

    while pages < CTGOV_MAX_PAGES:
        params: Dict[str, Any] = {
            "query.spons": sponsor_query,
            "pageSize": 1000,
        }
        if use_fields:
            params["fields"] = field_paths
        if page_token:
            params["pageToken"] = page_token

        try:
            data = http_get_json(
                base,
                headers=headers,
                params=params,
                throttle=CTGOV_THROTTLE_SECONDS,
                label=f"ctgov {sponsor_query}",
            )
        except SpikeError as exc:
            # If the fields syntax is rejected, retry once without it.
            if use_fields:
                print(f"    fields parameter rejected ({exc}); retrying without it")
                use_fields = False
                continue
            raise

        if not data:
            break

        batch = data.get("studies", [])
        studies.extend(batch)
        pages += 1

        page_token = data.get("nextPageToken")
        if not page_token or not batch:
            break

    return studies


def assign_phase(phases: Optional[List[str]]) -> str:
    """Highest phase in the array wins. Empty/NA -> 'NA'."""
    if not phases:
        return "NA"
    ranked = [p for p in phases if p in PHASE_RANK]
    if not ranked:
        return "NA"
    return max(ranked, key=lambda p: PHASE_RANK.index(p))


def parse_start_year(study_protocol: Dict[str, Any]) -> Tuple[Optional[int], bool]:
    """Returns (year, was_inferred)."""
    status = study_protocol.get("statusModule", {})
    raw = status.get("startDateStruct", {}).get("date")
    inferred = False
    if not raw:
        raw = status.get("studyFirstPostDateStruct", {}).get("date")
        inferred = True
    if not raw:
        return None, True
    try:
        return int(str(raw)[:4]), inferred
    except ValueError:
        return None, True


def summarise_studies(studies: List[Dict[str, Any]]) -> Dict[str, Any]:
    sponsor_counter: Counter = Counter()
    sponsor_class: Dict[str, str] = {}
    by_year_phase: Dict[int, Counter] = defaultdict(Counter)
    inferred_dates = 0
    industry_interventional = 0
    total_seen = 0

    for study in studies:
        proto = study.get("protocolSection", {})
        total_seen += 1

        design = proto.get("designModule", {})
        if design.get("studyType") != "INTERVENTIONAL":
            continue

        lead = proto.get("sponsorCollaboratorsModule", {}).get("leadSponsor", {})
        name = (lead.get("name") or "").strip()
        klass = lead.get("class") or "UNKNOWN"
        if not name:
            continue

        sponsor_counter[name] += 1
        sponsor_class[name] = klass

        if klass != "INDUSTRY":
            continue

        industry_interventional += 1
        year, was_inferred = parse_start_year(proto)
        if was_inferred:
            inferred_dates += 1
        if year is None:
            continue

        by_year_phase[year][assign_phase(design.get("phases"))] += 1

    return {
        "total_returned": total_seen,
        "industry_interventional": industry_interventional,
        "sponsor_counter": sponsor_counter,
        "sponsor_class": sponsor_class,
        "by_year_phase": by_year_phase,
        "inferred_dates": inferred_dates,
    }


def weighted_output(phase_counts: Counter) -> float:
    return sum(PHASE_WEIGHTS.get(p, 0.0) * n for p, n in phase_counts.items())


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------

def fmt_usd(v: Optional[float]) -> str:
    if v is None:
        return "not reported"
    if abs(v) >= 1e9:
        return f"${v / 1e9:,.2f}B"
    return f"${v / 1e6:,.1f}M"


def main() -> int:
    out: List[str] = []

    def w(line: str = "") -> None:
        out.append(line)
        print(line, flush=True)

    try:
        ua = get_user_agent()
    except SpikeError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return 1

    sec_headers = {"User-Agent": ua, "Accept-Encoding": "gzip, deflate"}

    w("# PharmaPulse - Phase 0 Feasibility Report")
    w()
    w(f"Generated: {datetime.utcnow().isoformat(timespec='seconds')}Z")
    w(f"Lag used for efficiency metric: {DEFAULT_LAG_YEARS} years")
    w()

    # ---- Q1 --------------------------------------------------------------
    w("## Q1. SEC EDGAR - R&D tag coverage")
    w()

    try:
        ticker_map = load_ticker_to_cik(sec_headers)
    except SpikeError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return 1
    w(f"Loaded SEC ticker map: {len(ticker_map):,} tickers.")
    w()

    financials: Dict[str, Dict[int, Dict[str, Any]]] = {}
    tags_used: Dict[str, Optional[str]] = {}

    w("| Company | Ticker | CIK | Tag used | Years found | Range | Latest R&D |")
    w("|---|---|---|---|---|---|---|")

    q1_notes: List[str] = []

    for company in TEST_COMPANIES:
        ticker = company["ticker"]
        cik = ticker_map.get(ticker)
        if not cik:
            w(f"| {company['name']} | {ticker} | NOT FOUND | - | 0 | - | - |")
            q1_notes.append(f"{ticker}: ticker not present in the SEC ticker map.")
            continue

        try:
            facts = fetch_company_facts(cik, sec_headers)
        except SpikeError as exc:
            w(f"| {company['name']} | {ticker} | {cik} | ERROR | 0 | - | - |")
            q1_notes.append(f"{ticker}: {exc}")
            continue

        if not facts:
            w(f"| {company['name']} | {ticker} | {cik} | NO FACTS | 0 | - | - |")
            continue

        by_year, tag, notes = extract_annual_rd(facts)
        financials[ticker] = by_year
        tags_used[ticker] = tag
        for n in notes:
            q1_notes.append(f"{ticker}: {n}")

        if by_year:
            years = sorted(by_year)
            latest = by_year[years[-1]]["value"]
            w(
                f"| {company['name']} | {ticker} | {cik} | "
                f"{tag} | {len(years)} | {years[0]}-{years[-1]} | {fmt_usd(latest)} |"
            )
        else:
            w(f"| {company['name']} | {ticker} | {cik} | NONE | 0 | - | - |")

    w()
    if q1_notes:
        w("**Q1 notes:**")
        w()
        for n in q1_notes:
            w(f"- {n}")
        w()

    covered = sum(
        1 for t, y in financials.items()
        if len([yr for yr in y if ANALYSIS_START_YEAR <= yr <= ANALYSIS_END_YEAR]) >= 8
    )
    w(
        f"**Q1 VERDICT:** {covered} of {len(TEST_COMPANIES)} companies have 8+ years "
        f"of annual R&D data in {ANALYSIS_START_YEAR}-{ANALYSIS_END_YEAR}."
    )
    w("Pass condition: 4 of 5. Below that, the company universe needs rethinking.")
    w()
    w("---")
    w()

    # ---- Q2 --------------------------------------------------------------
    w("## Q2. ClinicalTrials.gov - sponsor name variants")
    w()
    w(
        "This is the critical unknown. The table below is the seed data for the "
        "`sponsor_aliases` table. Every row is a real sponsor string that must be "
        "mapped by hand to a company."
    )
    w()

    trial_data: Dict[str, Dict[str, Any]] = {}

    for company in TEST_COMPANIES:
        ticker = company["ticker"]
        query = company["ctgov_query"]
        print(f"  fetching trials for {company['name']} (query: {query})...", flush=True)

        try:
            studies = fetch_ctgov_studies(query)
        except SpikeError as exc:
            w(f"### {company['name']} ({ticker})")
            w()
            w(f"ERROR: {exc}")
            w()
            continue

        summary = summarise_studies(studies)
        trial_data[ticker] = summary

        counter: Counter = summary["sponsor_counter"]
        klass = summary["sponsor_class"]
        industry_total = sum(
            n for name, n in counter.items() if klass.get(name) == "INDUSTRY"
        )
        top = counter.most_common(1)
        top_share = (top[0][1] / industry_total * 100) if top and industry_total else 0.0

        w(f"### {company['name']} ({ticker})")
        w()
        w(f"- Studies returned by query: **{summary['total_returned']:,}**")
        w(f"- Interventional + industry-sponsored: **{summary['industry_interventional']:,}**")
        w(f"- Distinct lead-sponsor strings: **{len(counter):,}**")
        w(f"- Share captured by the single most common string: **{top_share:.1f}%**")
        w(f"- Trials needing an inferred start date: **{summary['inferred_dates']:,}**")
        w()
        w("| Lead sponsor string (verbatim) | Class | Trials |")
        w("|---|---|---|")
        for name, count in counter.most_common(25):
            safe = name.replace("|", "\\|")
            w(f"| {safe} | {klass.get(name, '?')} | {count} |")
        if len(counter) > 25:
            w(f"| _...and {len(counter) - 25} more distinct strings_ | | |")
        w()

    w("**Q2 VERDICT:** read the 'share captured by the single most common string' "
      "figure for each company.")
    w()
    w("- **70%+** - exact matching plus a handful of curated aliases is enough. Proceed as specified.")
    w("- **40-70%** - subsidiary mapping is doing real work. Proceed, but budget a full session on the alias table and cut the universe to ~15 companies.")
    w("- **Under 40%** - the join is fragile. Shrink to 8-10 companies and treat the alias table as the main deliverable.")
    w()
    w("---")
    w()

    # ---- Q3 --------------------------------------------------------------
    w("## Q3. Does the efficiency metric produce a useful spread?")
    w()
    w(
        f"Metric: phase-weighted trial starts in year Y divided by R&D expense in "
        f"year Y-{DEFAULT_LAG_YEARS}, per $1M. Phase weights: "
        + ", ".join(f"{k}={v}" for k, v in PHASE_WEIGHTS.items())
    )
    w()
    w("**These weights are assumptions, not measured values.**")
    w()

    years_to_show = list(range(ANALYSIS_END_YEAR - 5, ANALYSIS_END_YEAR + 1))
    header = "| Company | " + " | ".join(str(y) for y in years_to_show) + " |"
    w(header)
    w("|---" * (len(years_to_show) + 1) + "|")

    all_ratios: List[float] = []

    for company in TEST_COMPANIES:
        ticker = company["ticker"]
        by_year_phase = trial_data.get(ticker, {}).get("by_year_phase", {})
        fin = financials.get(ticker, {})

        cells: List[str] = []
        for year in years_to_show:
            phase_counts = by_year_phase.get(year)
            rd_rec = fin.get(year - DEFAULT_LAG_YEARS)
            if not phase_counts or not rd_rec or not rd_rec.get("value"):
                cells.append("-")
                continue
            rd_musd = rd_rec["value"] / 1e6
            if rd_musd <= 0:
                cells.append("-")
                continue
            ratio = weighted_output(phase_counts) / rd_musd
            all_ratios.append(ratio)
            cells.append(f"{ratio:.4f}")

        w(f"| {company['name']} | " + " | ".join(cells) + " |")

    w()

    if len(all_ratios) >= 5:
        lo, hi = min(all_ratios), max(all_ratios)
        srt = sorted(all_ratios)
        median = srt[len(srt) // 2]
        spread = (hi / lo) if lo > 0 else float("inf")
        w(f"- Observations: **{len(all_ratios)}**")
        w(f"- Min / median / max: **{lo:.4f} / {median:.4f} / {hi:.4f}**")
        w(f"- Max-to-min ratio: **{spread:.1f}x**")
        w()
        if spread >= 3:
            w("**Q3 VERDICT: PASS.** There is real variation between companies. "
              "The metric distinguishes them, so the product has something to show.")
        elif spread >= 1.8:
            w("**Q3 VERDICT: MARGINAL.** Some spread, but weak. Consider reframing the "
              "headline around pipeline mix over time rather than cross-company ranking.")
        else:
            w("**Q3 VERDICT: FAIL.** Every company lands in the same band. The "
              "cross-company comparison has no story. Reframe before building UI.")
    else:
        w("**Q3 VERDICT: INCONCLUSIVE.** Too few computable company-years. "
          "Check the Q1 and Q2 sections above for the cause.")

    w()
    w("---")
    w()
    w("## What to do next")
    w()
    w("Paste this entire report back into the chat. Do not start building the "
      "database until the three verdicts above have been reviewed.")

    with open("phase0_report.md", "w", encoding="utf-8") as fh:
        fh.write("\n".join(out))

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SpikeError as exc:
        print(f"\nFATAL: {exc}", file=sys.stderr)
        sys.exit(1)
