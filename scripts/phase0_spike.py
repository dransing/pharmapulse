"""
PharmaPulse - Phase 0 Feasibility Spike (v2, corrected)
=======================================================

CHANGES FROM v1 - and why they matter
-------------------------------------
v1 derived the fiscal year from XBRL's `fy` field. That field is the fiscal
year of the FILING, not of the data point. A 10-K filed in 2025 contains
comparative figures for FY2024, FY2023 and FY2022, and every one of them is
tagged fy=2025, fp=FY, form=10-K. v1 therefore collapsed three distinct years
into one slot and kept whichever record it happened to see first. The "34
duplicate fiscal-year records" it reported were the symptom, not restatements.

v2 fixes this by:
  1. Deriving the fiscal year from each fact's `end` (period end) date.
  2. Deduplicating on the exact period_end date, keeping the most recently
     FILED value - which is the correct handling of genuine restatements.
  3. Auditing EVERY candidate R&D tag per company rather than silently taking
     the first that works, so tag-comparability problems are visible.
  4. Printing the last 8 years of actual values so the output can be
     eyeballed against reality.
  5. Attributing trials only to sponsor strings that actually belong to the
     company, and emitting those strings as sponsor_aliases seed rows.

Environment variables:
  SEC_USER_AGENT  (required)  e.g. "PharmaPulse Research you@example.com"
"""

from __future__ import annotations

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

TEST_COMPANIES: List[Dict[str, Any]] = [
    {"ticker": "PFE",  "name": "Pfizer",    "ctgov_query": "Pfizer",
     "alias_keywords": ["pfizer", "wyeth", "seagen", "array biopharma", "hospira"]},
    {"ticker": "MRK",  "name": "Merck",     "ctgov_query": "Merck Sharp Dohme",
     "alias_keywords": ["merck sharp", "merck & co", "arqule", "organon"]},
    {"ticker": "LLY",  "name": "Eli Lilly", "ctgov_query": "Eli Lilly",
     "alias_keywords": ["eli lilly", "loxo oncology", "dice therapeutics",
                        "dermira", "morphic therapeutic"]},
    {"ticker": "VRTX", "name": "Vertex",    "ctgov_query": "Vertex Pharmaceuticals",
     "alias_keywords": ["vertex pharmaceuticals"]},
    {"ticker": "INCY", "name": "Incyte",    "ctgov_query": "Incyte",
     "alias_keywords": ["incyte"]},
]

PHASE_WEIGHTS: Dict[str, float] = {
    "EARLY_PHASE1": 0.5,
    "PHASE1": 1.0,
    "PHASE2": 3.0,
    "PHASE3": 9.0,
    "PHASE4": 2.0,
}

PHASE_RANK = ["EARLY_PHASE1", "PHASE1", "PHASE2", "PHASE3", "PHASE4"]

RD_TAG_PRIMARY = "ResearchAndDevelopmentExpense"
RD_TAG_CANDIDATES = [
    RD_TAG_PRIMARY,
    "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost",
]

DEFAULT_LAG_YEARS = 2
ANALYSIS_START_YEAR = 2014
ANALYSIS_END_YEAR = 2024
MIN_YEARS_REQUIRED = 8

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
    Assign a fiscal year label from the period END date.

    A period ending Jun-Dec belongs to that calendar year. A period ending
    Jan-May belongs to the previous calendar year, since most of the period
    fell in it. This is what makes a Dec-FYE filer comparable to a Jan-FYE one.
    """
    return end.year if end.month >= 6 else end.year - 1


def extract_tag_series(
    facts: Dict[str, Any], tag: str
) -> Tuple[Dict[int, Dict[str, Any]], int]:
    """
    Extract annual 10-K values for one XBRL tag.

    Returns (by_fiscal_year, value_changing_restatement_count).

    Cleaning rules:
      - form == 10-K only
      - annual duration only (350-380 days)
      - fiscal year derived from the period END date, never the `fy` field
      - deduplicated on exact period_end, keeping the most recently filed value
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
                # Mark the retained record too - the filings disagree on this
                # period regardless of which one we end up keeping.
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


def audit_all_tags(facts: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Run extraction for every candidate tag so coverage can be compared."""
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    discovered = [
        t for t in us_gaap
        if "ResearchAndDevelopment" in t
        and not any(x in t for x in ("Asset", "Liability", "Payable", "Number"))
    ]
    tags = list(dict.fromkeys(RD_TAG_CANDIDATES + discovered))

    results: Dict[str, Dict[str, Any]] = {}
    for tag in tags:
        series, restatements = extract_tag_series(facts, tag)
        if not series:
            continue
        in_window = [y for y in series if ANALYSIS_START_YEAR <= y <= ANALYSIS_END_YEAR]
        results[tag] = {
            "series": series,
            "restatements": restatements,
            "years_total": len(series),
            "years_in_window": len(in_window),
            "min_year": min(series),
            "max_year": max(series),
        }
    return results


def choose_tag(audit: Dict[str, Dict[str, Any]]) -> Optional[str]:
    """Prefer the primary tag when coverage is adequate; else the widest."""
    if not audit:
        return None
    primary = audit.get(RD_TAG_PRIMARY)
    if primary and primary["years_in_window"] >= MIN_YEARS_REQUIRED:
        return RD_TAG_PRIMARY
    return max(audit.items(), key=lambda kv: kv[1]["years_in_window"])[0]


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


def summarise_studies(
    studies: List[Dict[str, Any]], alias_keywords: List[str]
) -> Dict[str, Any]:
    """Only counts trials whose LEAD sponsor string belongs to this company."""
    sponsor_counter: Counter = Counter()
    sponsor_class: Dict[str, str] = {}
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

        sponsor_counter[name] += 1
        sponsor_class[name] = klass

        if klass != "INDUSTRY" or not is_ours(name):
            continue

        matched += 1
        year, was_inferred = parse_start_year(proto)
        if was_inferred:
            inferred += 1
        if year is None:
            continue
        by_year_phase[year][assign_phase(design.get("phases"))] += 1

    alias_candidates = [
        (n, c) for n, c in sponsor_counter.most_common()
        if sponsor_class.get(n) == "INDUSTRY" and is_ours(n)
    ]

    return {
        "total_returned": len(studies),
        "matched_trials": matched,
        "by_year_phase": by_year_phase,
        "inferred_dates": inferred,
        "alias_candidates": alias_candidates,
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

    w("# PharmaPulse - Phase 0 Feasibility Report (v2, corrected)")
    w()
    w(f"Generated: {datetime.utcnow().isoformat(timespec='seconds')}Z")
    w()
    w("> v1 derived fiscal years from XBRL's `fy` field, which is the FILING's")
    w("> fiscal year, not the data point's. That collapsed several years into")
    w("> one slot. v2 derives the year from each fact's period END date.")
    w()

    # ---- Q1 --------------------------------------------------------------
    w("## Q1. SEC EDGAR - R&D tag coverage and values")
    w()

    try:
        ticker_map = load_ticker_to_cik(sec_headers)
    except SpikeError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return 1
    w(f"Loaded SEC ticker map: {len(ticker_map):,} tickers.")
    w()

    financials: Dict[str, Dict[int, Dict[str, Any]]] = {}
    chosen_tags: Dict[str, str] = {}

    for company in TEST_COMPANIES:
        ticker, cname = company["ticker"], company["name"]
        cik = ticker_map.get(ticker)
        w(f"### {cname} ({ticker})")
        w()

        if not cik:
            w("Ticker not found in the SEC ticker map.")
            w()
            continue

        try:
            facts = http_get_json(
                f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
                headers=sec_headers, throttle=SEC_THROTTLE_SECONDS,
                label=f"companyfacts {cik}",
            )
        except SpikeError as exc:
            w(f"ERROR: {exc}")
            w()
            continue

        if not facts:
            w("No company facts returned.")
            w()
            continue

        audit = audit_all_tags(facts)
        if not audit:
            w("No usable R&D tags found.")
            w()
            continue

        w(f"CIK {cik}. Candidate R&D tags:")
        w()
        w("| Tag | Years | In 2014-2024 | Range | Restatements |")
        w("|---|---|---|---|---|")
        for tag, info in sorted(audit.items(),
                                key=lambda kv: kv[1]["years_in_window"], reverse=True):
            w(f"| {tag} | {info['years_total']} | {info['years_in_window']} | "
              f"{info['min_year']}-{info['max_year']} | {info['restatements']} |")
        w()

        tag = choose_tag(audit)
        chosen_tags[ticker] = tag
        financials[ticker] = audit[tag]["series"]

        if tag != RD_TAG_PRIMARY:
            w(f"**Selected `{tag}` rather than the primary tag.** Comparability "
              "flag: values on different tags are not directly comparable "
              "across companies.")
            w()

        w("Last 8 fiscal years (sanity-check against reality):")
        w()
        w("| Fiscal year | Period end | R&D expense | Restated |")
        w("|---|---|---|---|")
        for y in sorted(financials[ticker])[-8:]:
            r = financials[ticker][y]
            w(f"| {y} | {r['period_end']} | {fmt_usd(r['value'])} | "
              f"{'yes' if r['restated'] else 'no'} |")
        w()

    covered = sum(
        1 for s in financials.values()
        if len([y for y in s if ANALYSIS_START_YEAR <= y <= ANALYSIS_END_YEAR])
        >= MIN_YEARS_REQUIRED
    )
    w(f"**Q1 VERDICT:** {covered} of {len(TEST_COMPANIES)} companies have "
      f"{MIN_YEARS_REQUIRED}+ years in {ANALYSIS_START_YEAR}-{ANALYSIS_END_YEAR}. "
      "Pass condition: 4 of 5.")
    w()

    mismatched = [t for t, tag in chosen_tags.items() if tag != RD_TAG_PRIMARY]
    if mismatched:
        w(f"**Tag comparability warning:** {', '.join(mismatched)} use a "
          "non-primary tag. Cross-company ratios involving them need a UI caveat.")
        w()

    w("---")
    w()

    # ---- Q2 --------------------------------------------------------------
    w("## Q2. ClinicalTrials.gov - sponsor aliases")
    w()
    w("Only lead sponsors matching this company's name patterns are counted. "
      "These tables are the seed rows for `sponsor_aliases`.")
    w()

    trial_data: Dict[str, Dict[str, Any]] = {}

    for company in TEST_COMPANIES:
        ticker = company["ticker"]
        print(f"  fetching trials for {company['name']}...", flush=True)
        try:
            studies = fetch_ctgov_studies(company["ctgov_query"])
        except SpikeError as exc:
            w(f"### {company['name']} ({ticker})")
            w()
            w(f"ERROR: {exc}")
            w()
            continue

        summary = summarise_studies(studies, company["alias_keywords"])
        trial_data[ticker] = summary

        aliases = summary["alias_candidates"]
        total = sum(c for _, c in aliases)
        top_share = (aliases[0][1] / total * 100) if aliases and total else 0.0

        w(f"### {company['name']} ({ticker})")
        w()
        w(f"- Trials attributed to this company: **{summary['matched_trials']:,}**")
        w(f"- Distinct sponsor strings: **{len(aliases)}**")
        w(f"- Share captured by the exact-name string: **{top_share:.1f}%**")
        w(f"- Trials needing an inferred start date: **{summary['inferred_dates']:,}**")
        w()
        w("| Sponsor string (verbatim) | Trials |")
        w("|---|---|")
        for name, count in aliases[:15]:
            w(f"| {name.replace('|', chr(92) + '|')} | {count} |")
        if len(aliases) > 15:
            w(f"| _...and {len(aliases) - 15} more_ | |")
        w()

    w("---")
    w()

    # ---- Q3 --------------------------------------------------------------
    w("## Q3. Efficiency metric spread (recomputed on corrected financials)")
    w()
    w(f"Phase-weighted trial starts in year Y / R&D in year Y-{DEFAULT_LAG_YEARS}, per $1M.")
    w(f"Weights: {', '.join(f'{k}={v}' for k, v in PHASE_WEIGHTS.items())} "
      "(assumptions, not measured values).")
    w()

    years = list(range(2018, 2025))
    w("| Company | " + " | ".join(str(y) for y in years) + " |")
    w("|---" * (len(years) + 1) + "|")

    all_ratios: List[float] = []
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
            cells.append(f"{ratio:.4f}")
        w(f"| {company['name']} | " + " | ".join(cells) + " |")

    w()
    if len(all_ratios) >= 5:
        lo, hi = min(all_ratios), max(all_ratios)
        srt = sorted(all_ratios)
        med = srt[len(srt) // 2]
        spread = hi / lo if lo > 0 else float("inf")
        w(f"- Observations: **{len(all_ratios)}**  |  Min / median / max: "
          f"**{lo:.4f} / {med:.4f} / {hi:.4f}**  |  Max-to-min: **{spread:.1f}x**")
        w()
        if spread >= 3:
            w("**Q3 VERDICT: PASS.** Real variation between companies.")
        elif spread >= 1.8:
            w("**Q3 VERDICT: MARGINAL.** Weak spread. Consider reframing around "
              "pipeline mix over time rather than cross-company ranking.")
        else:
            w("**Q3 VERDICT: FAIL.** No cross-company story. Reframe before UI.")
    else:
        w("**Q3 VERDICT: INCONCLUSIVE.** Too few computable company-years.")

    w()
    w("---")
    w()
    w("Paste this entire report back into the chat.")

    with open("phase0_report.md", "w", encoding="utf-8") as fh:
        fh.write("\n".join(out))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SpikeError as exc:
        print(f"\nFATAL: {exc}", file=sys.stderr)
        sys.exit(1)
