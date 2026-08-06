"""
PharmaPulse - Phase 0 Feasibility Spike (v3, final)
===================================================

CHANGE LOG
----------
v1 -> v2:
  Derived the fiscal year from each fact's period END date instead of XBRL's
  `fy` field. `fy` is the FILING's fiscal year, so a 10-K filed in 2025 tags
  its FY2024, FY2023 and FY2022 comparatives all as fy=2025. v1 collapsed
  them into one slot.

v2 -> v3:
  1. TAG STITCHING. Companies switch XBRL tags mid-history. Eli Lilly reports
     ResearchAndDevelopmentExpense through 2022 then switches to
     ...ExcludingAcquiredInProcessCost; Vertex switches in 2021. v2 picked one
     tag and silently discarded the rest of the series. v3 stitches the two
     into a single series and records which tag supplied each year.
  2. ALLOWLIST INSTEAD OF AUTO-DISCOVERY. v2 discovered any tag containing
     "ResearchAndDevelopment", which pulled in
     IncomeTaxReconciliationNondeductibleExpenseResearchAndDevelopment (a tax
     reconciliation line), PaymentsToAcquireInProcessResearchAndDevelopment (a
     cash flow item) and ResearchAndDevelopmentInProcess (an IPR&D charge).
     None are R&D operating expense. Only two tags are now permitted.
  3. OVERLAP AGREEMENT CHECK. Where both tags cover the same year, the values
     are compared. Large disagreement means the stitch is unsafe and the
     company needs a manual decision.
  4. DIVESTITURE FLAGGING. Sponsor strings describing a spin-off or merger
     away from the parent (e.g. Pfizer's Upjohn -> Viatris) are excluded from
     attribution and reported separately for effective-dating review.
  5. Universe expanded from 5 to 12 companies, and the run now emits
     ready-to-load seed data for the `companies` and `sponsor_aliases` tables.

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
# Company universe
# --------------------------------------------------------------------------
# `alias_keywords` are lowercase substrings that identify a lead-sponsor string
# as belonging to this company, including known acquired subsidiaries.
# Keep them specific: "merck sharp" not "merck", or Merck KGaA / EMD Serono
# (an unrelated German company) gets swept in.

TEST_COMPANIES: List[Dict[str, Any]] = [
    {"ticker": "PFE", "name": "Pfizer", "ctgov_query": "Pfizer",
     "alias_keywords": ["pfizer", "wyeth", "seagen", "array biopharma",
                        "hospira", "metsera"]},
    {"ticker": "MRK", "name": "Merck & Co", "ctgov_query": "Merck Sharp Dohme",
     "alias_keywords": ["merck sharp", "merck & co", "arqule", "organon",
                        "novacardia", "immune design", "acceleron",
                        "prometheus biosciences"]},
    {"ticker": "LLY", "name": "Eli Lilly", "ctgov_query": "Eli Lilly",
     "alias_keywords": ["eli lilly", "loxo oncology", "dice therapeutics",
                        "dermira", "morphic therapeutic", "point biopharma",
                        "scorpion therapeutics"]},
    {"ticker": "ABBV", "name": "AbbVie", "ctgov_query": "AbbVie",
     "alias_keywords": ["abbvie", "allergan", "pharmacyclics", "stemcentrx",
                        "cerevel", "immunogen"]},
    {"ticker": "BMY", "name": "Bristol-Myers Squibb",
     "ctgov_query": "Bristol-Myers Squibb",
     "alias_keywords": ["bristol-myers squibb", "bristol myers squibb",
                        "celgene", "juno therapeutics", "mirati",
                        "karuna therapeutics", "rayzebio", "turning point therapeutics"]},
    {"ticker": "AMGN", "name": "Amgen", "ctgov_query": "Amgen",
     "alias_keywords": ["amgen", "horizon therapeutics", "onyx pharmaceuticals",
                        "five prime", "chemocentryx"]},
    {"ticker": "GILD", "name": "Gilead Sciences", "ctgov_query": "Gilead Sciences",
     "alias_keywords": ["gilead", "kite pharma", "immunomedics", "forty seven",
                        "cymabay"]},
    {"ticker": "REGN", "name": "Regeneron", "ctgov_query": "Regeneron",
     "alias_keywords": ["regeneron"]},
    {"ticker": "VRTX", "name": "Vertex Pharmaceuticals",
     "ctgov_query": "Vertex Pharmaceuticals",
     "alias_keywords": ["vertex pharmaceuticals"]},
    {"ticker": "INCY", "name": "Incyte", "ctgov_query": "Incyte",
     "alias_keywords": ["incyte"]},
    {"ticker": "BIIB", "name": "Biogen", "ctgov_query": "Biogen",
     "alias_keywords": ["biogen", "reata pharmaceuticals",
                        "human genome sciences"]},
    {"ticker": "MRNA", "name": "Moderna", "ctgov_query": "ModernaTX",
     "alias_keywords": ["moderna"]},
]

# Sponsor strings matching these describe an entity that LEFT the parent.
# They are excluded from attribution and reported for effective-dating review.
DIVESTITURE_MARKERS = [
    "viatris",
    "has merged with",
    "spun off",
    "now part of",
]

PHASE_WEIGHTS: Dict[str, float] = {
    "EARLY_PHASE1": 0.5,
    "PHASE1": 1.0,
    "PHASE2": 3.0,
    "PHASE3": 9.0,
    "PHASE4": 2.0,
}
PHASE_RANK = ["EARLY_PHASE1", "PHASE1", "PHASE2", "PHASE3", "PHASE4"]

# ONLY these two tags are R&D operating expense. No auto-discovery.
TAG_PRIMARY = "ResearchAndDevelopmentExpense"
TAG_EXCL_IPRD = "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost"
ALLOWED_RD_TAGS = [TAG_PRIMARY, TAG_EXCL_IPRD]

DEFAULT_LAG_YEARS = 2
ANALYSIS_START_YEAR = 2014
ANALYSIS_END_YEAR = 2024
MIN_YEARS_REQUIRED = 8
OVERLAP_TOLERANCE = 0.02  # 2% relative difference is treated as agreement

SEC_THROTTLE_SECONDS = 0.25
CTGOV_THROTTLE_SECONDS = 1.5
CTGOV_MAX_PAGES = 12
REQUEST_TIMEOUT = 60


class SpikeError(Exception):
    pass


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

def get_user_agent() -> str:
    ua = os.environ.get("SEC_USER_AGENT", "").strip()
    if not ua or "@" not in ua:
        raise SpikeError(
            "SEC_USER_AGENT is missing or contains no email address.\n"
            "Set it as a GitHub Actions secret, for example:\n"
            "  PharmaPulse Research yourname@example.com"
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
    delay = 2.0
    for attempt in range(1, max_attempts + 1):
        time.sleep(throttle)
        try:
            resp = requests.get(url, headers=headers, params=params,
                                timeout=REQUEST_TIMEOUT)
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
            raise SpikeError(
                f"{label}: HTTP 403. The User-Agent was rejected or the IP is "
                "temporarily rate-limited. Wait 10 minutes and re-run."
            )

        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt == max_attempts:
                raise SpikeError(f"{label}: HTTP {resp.status_code} after {attempt} attempts.")
            time.sleep(delay)
            delay *= 2
            continue

        raise SpikeError(f"{label}: unexpected HTTP {resp.status_code} from {url}")
    return None


# --------------------------------------------------------------------------
# SEC
# --------------------------------------------------------------------------

def load_ticker_to_cik(headers: Dict[str, str]) -> Dict[str, str]:
    data = http_get_json(
        "https://www.sec.gov/files/company_tickers.json",
        headers=headers, throttle=SEC_THROTTLE_SECONDS, label="SEC ticker map",
    )
    if not data:
        raise SpikeError("Could not load the SEC ticker map.")
    out: Dict[str, str] = {}
    for row in data.values():
        t = str(row.get("ticker", "")).upper()
        if t:
            out[t] = str(row.get("cik_str", "")).zfill(10)
    return out


def _parse_iso(d: Any) -> Optional[date]:
    try:
        return datetime.strptime(str(d), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def fiscal_year_from_period_end(end: date) -> int:
    """
    Fiscal year label from the period END date.

    Ending Jun-Dec -> that calendar year. Ending Jan-May -> previous calendar
    year, since most of the period fell in it. Makes Dec-FYE and Jan-FYE
    filers comparable.
    """
    return end.year if end.month >= 6 else end.year - 1


def extract_tag_series(
    facts: Dict[str, Any], tag: str
) -> Tuple[Dict[int, Dict[str, Any]], int]:
    """
    Annual 10-K values for one XBRL tag.

    Rules: 10-K only; annual duration (350-380 days); fiscal year from the
    period END date; deduplicated on exact period_end keeping the most
    recently FILED value. Returns (by_fiscal_year, value_changing_restatements).
    """
    node = facts.get("facts", {}).get("us-gaap", {}).get(tag)
    if not node:
        return {}, 0

    records = node.get("units", {}).get("USD", [])
    if not records:
        return {}, 0

    by_period_end: Dict[date, Dict[str, Any]] = {}
    restatements = 0

    for rec in records:
        if rec.get("form") != "10-K":
            continue

        start = _parse_iso(rec.get("start"))
        end = _parse_iso(rec.get("end"))
        if not start or not end:
            continue
        if not (350 <= (end - start).days <= 380):
            continue

        val = rec.get("val")
        if val is None:
            continue

        filed = _parse_iso(rec.get("filed")) or date.min
        existing = by_period_end.get(end)
        was_restated = False

        if existing:
            if existing["value"] != val:
                restatements += 1
                was_restated = True
                existing["restated"] = True
            else:
                was_restated = existing["restated"]
            if filed <= existing["filed"]:
                continue

        by_period_end[end] = {
            "value": val,
            "period_end": end,
            "filed": filed,
            "accession": rec.get("accn"),
            "restated": was_restated,
        }

    by_year: Dict[int, Dict[str, Any]] = {}
    for end, rec in by_period_end.items():
        fy = fiscal_year_from_period_end(end)
        prior = by_year.get(fy)
        if prior and prior["period_end"] >= end:
            continue
        rec = dict(rec)
        rec["fiscal_year"] = fy
        by_year[fy] = rec

    return by_year, restatements


def stitch_rd_series(
    facts: Dict[str, Any]
) -> Tuple[Dict[int, Dict[str, Any]], Dict[str, Any]]:
    """
    Build one continuous R&D series from the two allowed tags.

    Companies switch tags mid-history, so neither tag alone covers the full
    period. The primary tag wins where present; the IPR&D-excluding tag fills
    the remaining years. Each year records which tag supplied it.

    Also compares the two tags on overlapping years. Disagreement above
    OVERLAP_TOLERANCE means the stitch splices together two different
    definitions and the company needs a manual decision.
    """
    per_tag: Dict[str, Dict[int, Dict[str, Any]]] = {}
    restatements: Dict[str, int] = {}

    for tag in ALLOWED_RD_TAGS:
        series, rest = extract_tag_series(facts, tag)
        if series:
            per_tag[tag] = series
            restatements[tag] = rest

    primary = per_tag.get(TAG_PRIMARY, {})
    excl = per_tag.get(TAG_EXCL_IPRD, {})

    stitched: Dict[int, Dict[str, Any]] = {}
    for year in sorted(set(primary) | set(excl)):
        if year in primary:
            rec = dict(primary[year])
            rec["tag"] = TAG_PRIMARY
        else:
            rec = dict(excl[year])
            rec["tag"] = TAG_EXCL_IPRD
        stitched[year] = rec

    overlap_years = sorted(set(primary) & set(excl))
    disagreements: List[Tuple[int, float, float, float]] = []
    for year in overlap_years:
        a = primary[year]["value"]
        b = excl[year]["value"]
        denom = max(abs(a), abs(b), 1.0)
        rel = abs(a - b) / denom
        if rel > OVERLAP_TOLERANCE:
            disagreements.append((year, a, b, rel))

    tags_used = sorted({r["tag"] for r in stitched.values()})

    diagnostics = {
        "per_tag_coverage": {
            tag: {
                "years": len(s),
                "min_year": min(s),
                "max_year": max(s),
                "in_window": len([y for y in s
                                  if ANALYSIS_START_YEAR <= y <= ANALYSIS_END_YEAR]),
                "restatements": restatements.get(tag, 0),
            }
            for tag, s in per_tag.items()
        },
        "overlap_years": overlap_years,
        "disagreements": disagreements,
        "tags_used": tags_used,
        "is_spliced": len(tags_used) > 1,
    }
    return stitched, diagnostics


# --------------------------------------------------------------------------
# ClinicalTrials.gov
# --------------------------------------------------------------------------

def fetch_ctgov_studies(sponsor_query: str) -> List[Dict[str, Any]]:
    base = "https://clinicaltrials.gov/api/v2/studies"
    headers = {"Accept": "application/json", "User-Agent": get_user_agent()}

    field_paths = ",".join([
        "protocolSection.identificationModule.nctId",
        "protocolSection.sponsorCollaboratorsModule.leadSponsor",
        "protocolSection.designModule.phases",
        "protocolSection.designModule.studyType",
        "protocolSection.statusModule.overallStatus",
        "protocolSection.statusModule.startDateStruct",
        "protocolSection.statusModule.studyFirstPostDateStruct",
    ])

    studies: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    use_fields = True
    pages = 0

    while pages < CTGOV_MAX_PAGES:
        params: Dict[str, Any] = {"query.spons": sponsor_query, "pageSize": 1000}
        if use_fields:
            params["fields"] = field_paths
        if page_token:
            params["pageToken"] = page_token

        try:
            data = http_get_json(
                base, headers=headers, params=params,
                throttle=CTGOV_THROTTLE_SECONDS, label=f"ctgov {sponsor_query}",
            )
        except SpikeError:
            if use_fields:
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
    if not phases:
        return "NA"
    ranked = [p for p in phases if p in PHASE_RANK]
    if not ranked:
        return "NA"
    return max(ranked, key=lambda p: PHASE_RANK.index(p))


def parse_start_year(proto: Dict[str, Any]) -> Tuple[Optional[int], bool]:
    status = proto.get("statusModule", {})
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


def is_divestiture(name: str) -> bool:
    low = name.lower()
    return any(m in low for m in DIVESTITURE_MARKERS)


def summarise_studies(
    studies: List[Dict[str, Any]], alias_keywords: List[str]
) -> Dict[str, Any]:
    """
    Attributes a trial to the company only when the LEAD sponsor string matches
    one of its name patterns and is not a divested entity.
    """
    counter: Counter = Counter()
    klass_map: Dict[str, str] = {}
    by_year_phase: Dict[int, Counter] = defaultdict(Counter)
    inferred = 0
    matched = 0

    keywords = [k.lower() for k in alias_keywords]

    def is_ours(name: str) -> bool:
        low = name.lower()
        return any(k in low for k in keywords)

    for study in studies:
        proto = study.get("protocolSection", {})
        design = proto.get("designModule", {})
        if design.get("studyType") != "INTERVENTIONAL":
            continue

        lead = proto.get("sponsorCollaboratorsModule", {}).get("leadSponsor", {})
        name = (lead.get("name") or "").strip()
        klass = lead.get("class") or "UNKNOWN"
        if not name:
            continue

        counter[name] += 1
        klass_map[name] = klass

        if klass != "INDUSTRY" or not is_ours(name) or is_divestiture(name):
            continue

        matched += 1
        year, was_inferred = parse_start_year(proto)
        if was_inferred:
            inferred += 1
        if year is None:
            continue
        by_year_phase[year][assign_phase(design.get("phases"))] += 1

    ours = [(n, c) for n, c in counter.most_common()
            if klass_map.get(n) == "INDUSTRY" and is_ours(n)]
    aliases = [(n, c) for n, c in ours if not is_divestiture(n)]
    divested = [(n, c) for n, c in ours if is_divestiture(n)]

    return {
        "total_returned": len(studies),
        "matched_trials": matched,
        "by_year_phase": by_year_phase,
        "inferred_dates": inferred,
        "alias_candidates": aliases,
        "divested_candidates": divested,
    }


def weighted_output(counts: Counter) -> float:
    return sum(PHASE_WEIGHTS.get(p, 0.0) * n for p, n in counts.items())


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------

def fmt_usd(v: Optional[float]) -> str:
    if v is None:
        return "-"
    if abs(v) >= 1e9:
        return f"${v / 1e9:,.2f}B"
    return f"${v / 1e6:,.0f}M"


def esc(s: str) -> str:
    return s.replace("|", chr(92) + "|")


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

    w("# PharmaPulse - Phase 0 Feasibility Report (v3, final)")
    w()
    w(f"Generated: {datetime.utcnow().isoformat(timespec='seconds')}Z")
    w(f"Universe: {len(TEST_COMPANIES)} companies")
    w()
    w("> v3 stitches the two permitted R&D tags into one continuous series,")
    w("> restricts tags to an explicit allowlist, checks tag agreement on")
    w("> overlapping years, and excludes divested entities from attribution.")
    w()

    # ---- Q1 --------------------------------------------------------------
    w("## Q1. SEC EDGAR - stitched R&D series")
    w()

    try:
        ticker_map = load_ticker_to_cik(sec_headers)
    except SpikeError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return 1
    w(f"Loaded SEC ticker map: {len(ticker_map):,} tickers.")
    w()

    financials: Dict[str, Dict[int, Dict[str, Any]]] = {}
    diags: Dict[str, Dict[str, Any]] = {}
    ciks: Dict[str, str] = {}

    w("| Company | Ticker | Years 2014-2024 | Range | Tags used | Spliced | Overlap conflicts |")
    w("|---|---|---|---|---|---|---|")

    for company in TEST_COMPANIES:
        ticker, cname = company["ticker"], company["name"]
        cik = ticker_map.get(ticker)
        if not cik:
            w(f"| {cname} | {ticker} | - | - | TICKER NOT FOUND | - | - |")
            continue
        ciks[ticker] = cik

        try:
            facts = http_get_json(
                f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
                headers=sec_headers, throttle=SEC_THROTTLE_SECONDS,
                label=f"companyfacts {cik}",
            )
        except SpikeError as exc:
            w(f"| {cname} | {ticker} | - | - | ERROR | - | - |")
            print(f"    {exc}", file=sys.stderr)
            continue

        if not facts:
            w(f"| {cname} | {ticker} | - | - | NO FACTS | - | - |")
            continue

        series, diag = stitch_rd_series(facts)
        if not series:
            w(f"| {cname} | {ticker} | 0 | - | NONE FOUND | - | - |")
            continue

        financials[ticker] = series
        diags[ticker] = diag

        in_window = len([y for y in series
                         if ANALYSIS_START_YEAR <= y <= ANALYSIS_END_YEAR])
        short_tags = ", ".join(
            "primary" if t == TAG_PRIMARY else "excl-IPRD" for t in diag["tags_used"]
        )
        w(f"| {cname} | {ticker} | {in_window} | {min(series)}-{max(series)} | "
          f"{short_tags} | {'yes' if diag['is_spliced'] else 'no'} | "
          f"{len(diag['disagreements'])} |")

    w()

    # Detail per company
    for company in TEST_COMPANIES:
        ticker = company["ticker"]
        if ticker not in financials:
            continue
        series, diag = financials[ticker], diags[ticker]

        w(f"### {company['name']} ({ticker}) - CIK {ciks.get(ticker, '?')}")
        w()
        w("| Tag | Years | In 2014-2024 | Range | Restatements |")
        w("|---|---|---|---|---|")
        for tag, info in diag["per_tag_coverage"].items():
            label = "primary" if tag == TAG_PRIMARY else "excl-IPRD"
            w(f"| {label} | {info['years']} | {info['in_window']} | "
              f"{info['min_year']}-{info['max_year']} | {info['restatements']} |")
        w()

        if diag["disagreements"]:
            w("**Tag conflict on overlapping years.** The two tags report "
              "materially different values for the same period, so splicing "
              "them mixes definitions. This company needs a manual decision:")
            w()
            w("| Year | Primary | Excl-IPRD | Difference |")
            w("|---|---|---|---|")
            for y, a, b, rel in diag["disagreements"]:
                w(f"| {y} | {fmt_usd(a)} | {fmt_usd(b)} | {rel * 100:.1f}% |")
            w()
        elif diag["overlap_years"]:
            w(f"Tags agree on all {len(diag['overlap_years'])} overlapping "
              "years. Splice is safe.")
            w()

        w("| Fiscal year | Period end | R&D expense | Tag | Restated |")
        w("|---|---|---|---|---|")
        for y in sorted(series)[-10:]:
            r = series[y]
            label = "primary" if r["tag"] == TAG_PRIMARY else "excl-IPRD"
            w(f"| {y} | {r['period_end']} | {fmt_usd(r['value'])} | {label} | "
              f"{'yes' if r['restated'] else 'no'} |")
        w()

    covered = sum(
        1 for s in financials.values()
        if len([y for y in s if ANALYSIS_START_YEAR <= y <= ANALYSIS_END_YEAR])
        >= MIN_YEARS_REQUIRED
    )
    w(f"**Q1 VERDICT:** {covered} of {len(TEST_COMPANIES)} companies have "
      f"{MIN_YEARS_REQUIRED}+ years in {ANALYSIS_START_YEAR}-{ANALYSIS_END_YEAR}.")
    w("Pass condition: 10 of 12.")
    w()

    spliced = [t for t, d in diags.items() if d["is_spliced"]]
    conflicted = [t for t, d in diags.items() if d["disagreements"]]
    if spliced:
        w(f"- Spliced series (two tags stitched): {', '.join(spliced)}")
    if conflicted:
        w(f"- **Needs manual decision (tag conflict): {', '.join(conflicted)}**")
    w()
    w("---")
    w()

    # ---- Q2 --------------------------------------------------------------
    w("## Q2. ClinicalTrials.gov - sponsor aliases")
    w()

    trial_data: Dict[str, Dict[str, Any]] = {}
    all_alias_rows: List[Tuple[str, str, int]] = []
    all_divested: List[Tuple[str, str, int]] = []

    for company in TEST_COMPANIES:
        ticker = company["ticker"]
        print(f"  fetching trials for {company['name']}...", flush=True)
        try:
            studies = fetch_ctgov_studies(company["ctgov_query"])
        except SpikeError as exc:
            w(f"### {company['name']} ({ticker}) - ERROR: {exc}")
            w()
            continue

        summary = summarise_studies(studies, company["alias_keywords"])
        trial_data[ticker] = summary

        aliases = summary["alias_candidates"]
        divested = summary["divested_candidates"]
        total = sum(c for _, c in aliases)
        top_share = (aliases[0][1] / total * 100) if aliases and total else 0.0

        for name, count in aliases:
            all_alias_rows.append((ticker, name, count))
        for name, count in divested:
            all_divested.append((ticker, name, count))

        w(f"### {company['name']} ({ticker})")
        w()
        w(f"- Attributed trials: **{summary['matched_trials']:,}** | "
          f"alias strings: **{len(aliases)}** | "
          f"exact-name share: **{top_share:.1f}%** | "
          f"inferred dates: **{summary['inferred_dates']:,}**")
        w()
        w("| Sponsor string (verbatim) | Trials |")
        w("|---|---|")
        for name, count in aliases[:12]:
            w(f"| {esc(name)} | {count} |")
        if len(aliases) > 12:
            w(f"| _...and {len(aliases) - 12} more_ | |")
        w()

        if divested:
            w("Excluded as divested / spun off (needs effective-dating review):")
            w()
            w("| Sponsor string | Trials |")
            w("|---|---|")
            for name, count in divested:
                w(f"| {esc(name)} | {count} |")
            w()

    w(f"**Q2 VERDICT:** {len(all_alias_rows)} alias rows across "
      f"{len(trial_data)} companies. This is the manual curation workload.")
    w()
    w("---")
    w()

    # ---- Q3 --------------------------------------------------------------
    w("## Q3. Efficiency metric spread")
    w()
    w(f"Phase-weighted trial starts in year Y / R&D in year Y-{DEFAULT_LAG_YEARS}, per $1M.")
    w(f"Weights: {', '.join(f'{k}={v}' for k, v in PHASE_WEIGHTS.items())} "
      "(assumptions, not measured values).")
    w()

    years = list(range(2017, 2025))
    w("| Company | " + " | ".join(str(y) for y in years) + " |")
    w("|---" * (len(years) + 1) + "|")

    all_ratios: List[float] = []
    latest_by_company: List[Tuple[str, float]] = []

    for company in TEST_COMPANIES:
        ticker = company["ticker"]
        byp = trial_data.get(ticker, {}).get("by_year_phase", {})
        fin = financials.get(ticker, {})
        cells: List[str] = []
        for y in years:
            counts = byp.get(y)
            rd = fin.get(y - DEFAULT_LAG_YEARS)
            if not counts or not rd or not rd.get("value") or rd["value"] <= 0:
                cells.append("-")
                continue
            ratio = weighted_output(counts) / (rd["value"] / 1e6)
            all_ratios.append(ratio)
            if y == 2024:
                latest_by_company.append((company["name"], ratio))
            cells.append(f"{ratio:.4f}")
        w(f"| {company['name']} | " + " | ".join(cells) + " |")

    w()
    if len(all_ratios) >= 10:
        lo, hi = min(all_ratios), max(all_ratios)
        srt = sorted(all_ratios)
        med = srt[len(srt) // 2]
        spread = hi / lo if lo > 0 else float("inf")
        w(f"- Observations: **{len(all_ratios)}** | Min / median / max: "
          f"**{lo:.4f} / {med:.4f} / {hi:.4f}** | Max-to-min: **{spread:.1f}x**")
        w()
        if latest_by_company:
            latest_by_company.sort(key=lambda kv: kv[1], reverse=True)
            w("2024 ranking:")
            w()
            for i, (nm, r) in enumerate(latest_by_company, 1):
                w(f"{i}. {nm} - {r:.4f}")
            w()
        if spread >= 3:
            w("**Q3 VERDICT: PASS.** Real variation between companies.")
        elif spread >= 1.8:
            w("**Q3 VERDICT: MARGINAL.** Reframe around pipeline mix over time.")
        else:
            w("**Q3 VERDICT: FAIL.** No cross-company story.")
    else:
        w("**Q3 VERDICT: INCONCLUSIVE.** Too few computable company-years.")

    w()
    w("---")
    w()

    # ---- Seed data -------------------------------------------------------
    w("## Seed data for the database")
    w()
    w("Copy these blocks. They load directly into `companies` and "
      "`sponsor_aliases` in the next step.")
    w()
    w("### companies")
    w()
    w("```csv")
    w("ticker,cik,name")
    for company in TEST_COMPANIES:
        t = company["ticker"]
        if t in ciks:
            w(f"{t},{ciks[t]},\"{company['name']}\"")
    w("```")
    w()
    w("### sponsor_aliases")
    w()
    w("```csv")
    w("ticker,sponsor_name,trial_count,status")
    for ticker, name, count in all_alias_rows:
        w(f"{ticker},\"{name}\",{count},active")
    for ticker, name, count in all_divested:
        w(f"{ticker},\"{name}\",{count},needs_effective_dating")
    w("```")
    w()
    w("---")
    w()
    w("Paste this entire report back into the chat.")

    with open("phase0_report.md", "w", encoding="utf-8") as fh:
        fh.write("\n".join(out))

    with open("seed_data.json", "w", encoding="utf-8") as fh:
        json.dump(
            {
                "companies": [
                    {"ticker": c["ticker"], "cik": ciks.get(c["ticker"]),
                     "name": c["name"]}
                    for c in TEST_COMPANIES if c["ticker"] in ciks
                ],
                "aliases": [
                    {"ticker": t, "sponsor_name": n, "trial_count": c,
                     "status": "active"}
                    for t, n, c in all_alias_rows
                ] + [
                    {"ticker": t, "sponsor_name": n, "trial_count": c,
                     "status": "needs_effective_dating"}
                    for t, n, c in all_divested
                ],
            },
            fh, indent=2,
        )

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SpikeError as exc:
        print(f"\nFATAL: {exc}", file=sys.stderr)
        sys.exit(1)
