"""
PharmaPulse - Ingestion ETL
===========================

Runs in GitHub Actions. Reads the curated universe from Supabase, pulls SEC and
ClinicalTrials.gov data, and writes the serving tables.

Pipeline
--------
  1. Load companies + sponsor_aliases from Supabase.
  2. SEC: fetch companyfacts per CIK, stitch the R&D series, upsert
     financial_facts.
  3. ClinicalTrials.gov: query ONCE PER ALIAS STRING (not per company) and
     upsert trials.
  4. Map trials to companies, honouring each alias's effective_from /
     effective_to window.
  5. Compute company_year_metrics including all six lag variants.
  6. Record the run in ingestion_runs / ingestion_errors.

Why step 3 queries per alias
----------------------------
Querying only the parent company name under-counts acquisitive companies. The
Phase 0 spike found ONE Celgene trial under a "Bristol-Myers Squibb" search;
Celgene ran hundreds. A subsidiary's trials do not surface under the parent's
name unless the parent happens to be named in the record.

Why effective dating matters
----------------------------
Pfizer acquired Seagen in Dec 2023. A Seagen trial that started in 2019 was
funded by Seagen's R&D, not Pfizer's. Attributing it to Pfizer inflates
Pfizer's historical output. Same in reverse for divestitures: Upjohn trials
after the Nov 2020 Viatris spin-off are not Pfizer's.

Environment variables (GitHub Actions secrets)
----------------------------------------------
  SEC_USER_AGENT              e.g. "PharmaPulse Research you@example.com"
  SUPABASE_URL                e.g. "https://abcdefgh.supabase.co"
  SUPABASE_SERVICE_ROLE_KEY   service_role key - bypasses RLS, never client-side
"""

from __future__ import annotations

import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, date, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

PHASE_WEIGHTS: Dict[str, float] = {
    "EARLY_PHASE1": 0.5,
    "PHASE1": 1.0,
    "PHASE2": 3.0,
    "PHASE3": 9.0,
    "PHASE4": 2.0,
}
PHASE_RANK = ["EARLY_PHASE1", "PHASE1", "PHASE2", "PHASE3", "PHASE4"]

TAG_PRIMARY = "ResearchAndDevelopmentExpense"
TAG_EXCL_IPRD = "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost"

# Gilead's primary tag includes acquired IPR&D and covers fewer years than the
# excluding tag. Splicing them mixes definitions, so the excluding tag is used
# alone. See the Phase 0 report for the overlap conflict that motivated this.
TAG_OVERRIDES: Dict[str, str] = {
    "GILD": TAG_EXCL_IPRD,
}

MIN_TRIALS_FOR_RATIO = 3   # below this the ratio is noise; metric suppressed
METRIC_START_YEAR = 2010
METRIC_END_YEAR = 2026
LAGS = [0, 1, 2, 3, 4, 5]

SEC_THROTTLE = 0.25        # 4 req/s, half the documented 10 req/s ceiling
CTGOV_THROTTLE = 1.5
CTGOV_MAX_PAGES = 20
REQUEST_TIMEOUT = 90
UPSERT_BATCH = 500


class EtlError(Exception):
    pass


# --------------------------------------------------------------------------
# Environment
# --------------------------------------------------------------------------

def require_env(name: str, must_contain: Optional[str] = None) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise EtlError(f"Missing required environment variable: {name}")
    if must_contain and must_contain not in val:
        raise EtlError(f"{name} looks wrong - expected it to contain '{must_contain}'")
    return val


# --------------------------------------------------------------------------
# Supabase (PostgREST) client
# --------------------------------------------------------------------------

class Supabase:
    def __init__(self, url: str, service_key: str):
        self.base = url.rstrip("/") + "/rest/v1"
        self.headers = {
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Content-Type": "application/json",
        }

    def select(self, table: str, params: Optional[Dict[str, str]] = None) -> List[Dict]:
        rows: List[Dict] = []
        offset = 0
        page = 1000
        while True:
            p = dict(params or {})
            p["limit"] = str(page)
            p["offset"] = str(offset)
            resp = requests.get(f"{self.base}/{table}", headers=self.headers,
                                params=p, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                raise EtlError(f"select {table} failed [{resp.status_code}]: {resp.text[:400]}")
            batch = resp.json()
            rows.extend(batch)
            if len(batch) < page:
                return rows
            offset += page

    def upsert(self, table: str, rows: List[Dict], on_conflict: str) -> int:
        if not rows:
            return 0
        total = 0
        headers = dict(self.headers)
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
        for i in range(0, len(rows), UPSERT_BATCH):
            chunk = rows[i:i + UPSERT_BATCH]
            resp = requests.post(
                f"{self.base}/{table}",
                headers=headers,
                params={"on_conflict": on_conflict},
                json=chunk,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code not in (200, 201, 204):
                raise EtlError(
                    f"upsert {table} failed [{resp.status_code}]: {resp.text[:400]}"
                )
            total += len(chunk)
        return total

    def insert_one(self, table: str, row: Dict) -> Dict:
        headers = dict(self.headers)
        headers["Prefer"] = "return=representation"
        resp = requests.post(f"{self.base}/{table}", headers=headers,
                             json=row, timeout=REQUEST_TIMEOUT)
        if resp.status_code not in (200, 201):
            raise EtlError(f"insert {table} failed [{resp.status_code}]: {resp.text[:400]}")
        data = resp.json()
        return data[0] if isinstance(data, list) else data

    def patch(self, table: str, match: Dict[str, str], row: Dict) -> None:
        params = {k: f"eq.{v}" for k, v in match.items()}
        headers = dict(self.headers)
        headers["Prefer"] = "return=minimal"
        resp = requests.patch(f"{self.base}/{table}", headers=headers,
                              params=params, json=row, timeout=REQUEST_TIMEOUT)
        if resp.status_code not in (200, 204):
            raise EtlError(f"patch {table} failed [{resp.status_code}]: {resp.text[:400]}")


# --------------------------------------------------------------------------
# HTTP with backoff
# --------------------------------------------------------------------------

def http_get_json(url: str, headers: Dict[str, str],
                  params: Optional[Dict[str, Any]] = None,
                  throttle: float = 0.25, max_attempts: int = 5,
                  label: str = "") -> Optional[Any]:
    delay = 2.0
    for attempt in range(1, max_attempts + 1):
        time.sleep(throttle)
        try:
            resp = requests.get(url, headers=headers, params=params,
                                timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            if attempt == max_attempts:
                raise EtlError(f"{label}: network failure: {exc}")
            time.sleep(delay)
            delay *= 2
            continue

        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError as exc:
                raise EtlError(f"{label}: invalid JSON: {exc}")
        if resp.status_code == 404:
            return None
        if resp.status_code == 403:
            raise EtlError(f"{label}: HTTP 403 - User-Agent rejected or IP rate-limited.")
        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt == max_attempts:
                raise EtlError(f"{label}: HTTP {resp.status_code} after {attempt} attempts")
            time.sleep(delay)
            delay *= 2
            continue
        raise EtlError(f"{label}: HTTP {resp.status_code}")
    return None


# --------------------------------------------------------------------------
# SEC extraction (identical logic to the validated Phase 0 spike)
# --------------------------------------------------------------------------

def parse_iso(d: Any) -> Optional[date]:
    try:
        return datetime.strptime(str(d), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def fiscal_year_from_period_end(end: date) -> int:
    """Jun-Dec end -> that year. Jan-May end -> previous year."""
    return end.year if end.month >= 6 else end.year - 1


def extract_tag_series(facts: Dict[str, Any], tag: str) -> Dict[int, Dict[str, Any]]:
    """
    Annual 10-K values for one XBRL tag.

    The fiscal year comes from the period END date, never from the `fy` field.
    `fy` is the FILING's fiscal year, so a 10-K filed in 2025 tags its 2024,
    2023 and 2022 comparatives all as fy=2025.
    """
    node = facts.get("facts", {}).get("us-gaap", {}).get(tag)
    if not node:
        return {}
    records = node.get("units", {}).get("USD", [])
    if not records:
        return {}

    by_period_end: Dict[date, Dict[str, Any]] = {}
    for rec in records:
        if rec.get("form") != "10-K":
            continue
        start, end = parse_iso(rec.get("start")), parse_iso(rec.get("end"))
        if not start or not end:
            continue
        if not (350 <= (end - start).days <= 380):
            continue
        val = rec.get("val")
        if val is None:
            continue

        filed = parse_iso(rec.get("filed")) or date.min
        existing = by_period_end.get(end)
        restated = False
        if existing:
            if existing["value"] != val:
                restated = True
                existing["restated"] = True
            else:
                restated = existing["restated"]
            if filed <= existing["filed"]:
                continue

        by_period_end[end] = {
            "value": val, "period_end": end, "filed": filed,
            "accession": rec.get("accn"), "restated": restated,
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
    return by_year


def build_rd_series(facts: Dict[str, Any], ticker: str) -> Dict[int, Dict[str, Any]]:
    """
    One continuous R&D series.

    Preference order: the excluding-IPR&D tag where available, then the primary
    tag. The excluding tag is the cleaner operating-R&D measure and is what the
    majority of the universe reports, so preferring it maximises cross-company
    consistency. TAG_OVERRIDES pins a single tag where splicing would mix
    definitions.
    """
    override = TAG_OVERRIDES.get(ticker)
    if override:
        series = extract_tag_series(facts, override)
        for rec in series.values():
            rec["tag"] = override
        return series

    excl = extract_tag_series(facts, TAG_EXCL_IPRD)
    primary = extract_tag_series(facts, TAG_PRIMARY)

    out: Dict[int, Dict[str, Any]] = {}
    for year in sorted(set(excl) | set(primary)):
        if year in excl:
            rec = dict(excl[year]); rec["tag"] = TAG_EXCL_IPRD
        else:
            rec = dict(primary[year]); rec["tag"] = TAG_PRIMARY
        out[year] = rec

    # A year is "spliced" when it uses a tag other than the company's dominant
    # one. Vertex pre-2020 is the canonical case.
    if out:
        dominant = Counter(r["tag"] for r in out.values()).most_common(1)[0][0]
        for rec in out.values():
            rec["spliced"] = rec["tag"] != dominant
    return out


def rd_definition_for_tag(tag: str) -> str:
    return "excludes_iprd" if tag == TAG_EXCL_IPRD else "includes_iprd"


# --------------------------------------------------------------------------
# ClinicalTrials.gov
# --------------------------------------------------------------------------

def assign_phase(phases: Optional[List[str]]) -> str:
    if not phases:
        return "NA"
    ranked = [p for p in phases if p in PHASE_RANK]
    if not ranked:
        return "NA"
    return max(ranked, key=lambda p: PHASE_RANK.index(p))


def parse_start_date(proto: Dict[str, Any]) -> Tuple[Optional[date], bool]:
    """Returns (start_date, was_inferred). YYYY-MM is padded to day 01."""
    status = proto.get("statusModule", {})
    raw = status.get("startDateStruct", {}).get("date")
    inferred = False
    if not raw:
        raw = status.get("studyFirstPostDateStruct", {}).get("date")
        inferred = True
    if not raw:
        return None, True
    s = str(raw)
    if len(s) == 7:
        s += "-01"
    parsed = parse_iso(s)
    if parsed is None:
        return None, True
    return parsed, inferred


def fetch_trials_for_sponsor(sponsor_name: str, ua: str) -> List[Dict[str, Any]]:
    """Query by exact sponsor string, then keep only exact lead-sponsor matches."""
    base = "https://clinicaltrials.gov/api/v2/studies"
    headers = {"Accept": "application/json", "User-Agent": ua}
    fields = ",".join([
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

    collected: List[Dict[str, Any]] = []
    token: Optional[str] = None
    use_fields = True
    pages = 0

    while pages < CTGOV_MAX_PAGES:
        params: Dict[str, Any] = {"query.spons": sponsor_name, "pageSize": 1000}
        if use_fields:
            params["fields"] = fields
        if token:
            params["pageToken"] = token
        try:
            data = http_get_json(base, headers, params, CTGOV_THROTTLE,
                                 label=f"ctgov '{sponsor_name[:40]}'")
        except EtlError:
            if use_fields:
                use_fields = False
                continue
            raise
        if not data:
            break
        batch = data.get("studies", [])
        collected.extend(batch)
        pages += 1
        token = data.get("nextPageToken")
        if not token or not batch:
            break

    out: List[Dict[str, Any]] = []
    for study in collected:
        proto = study.get("protocolSection", {})
        design = proto.get("designModule", {})
        if design.get("studyType") != "INTERVENTIONAL":
            continue
        lead = proto.get("sponsorCollaboratorsModule", {}).get("leadSponsor", {})
        name = (lead.get("name") or "").strip()
        if name != sponsor_name:
            continue

        nct = proto.get("identificationModule", {}).get("nctId")
        if not nct:
            continue
        start, inferred = parse_start_date(proto)
        enroll = design.get("enrollmentInfo") or {}
        out.append({
            "nct_id": nct,
            "lead_sponsor_name": name,
            "lead_sponsor_class": lead.get("class"),
            "study_type": design.get("studyType"),
            "phase_raw": design.get("phases") or [],
            "phase_assigned": assign_phase(design.get("phases")),
            "overall_status": proto.get("statusModule", {}).get("overallStatus"),
            "start_date": start.isoformat() if start else None,
            "date_inferred": inferred,
            "enrollment_count": enroll.get("count"),
            "enrollment_type": enroll.get("type"),
            "conditions": proto.get("conditionsModule", {}).get("conditions") or [],
            "last_ingested_at": datetime.now(timezone.utc).isoformat(),
        })
    return out


# --------------------------------------------------------------------------
# Mapping and metrics
# --------------------------------------------------------------------------

def alias_covers(trial_start: Optional[date],
                 eff_from: Optional[date],
                 eff_to: Optional[date]) -> bool:
    """
    Does a trial starting on trial_start fall inside this alias's window?

    A trial with no usable start date is attributed (dropping it would lose
    real activity), but it is counted toward pct_dates_inferred so the
    confidence rating reflects the uncertainty.
    """
    if trial_start is None:
        return True
    if eff_from and trial_start < eff_from:
        return False
    if eff_to and trial_start > eff_to:
        return False
    return True


def weighted_output(counts: Counter) -> float:
    return sum(PHASE_WEIGHTS.get(p, 0.0) * n for p, n in counts.items())


def rate_confidence(n_trials: int, pct_inferred: float, spliced: bool) -> str:
    if n_trials < MIN_TRIALS_FOR_RATIO or pct_inferred > 0.30:
        return "low"
    if spliced or n_trials < 10 or pct_inferred > 0:
        return "medium"
    return "high"


def compute_metrics(
    company_id: str,
    rd_by_year: Dict[int, Dict[str, Any]],
    trials_by_year: Dict[int, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    years = sorted(
        {y for y in rd_by_year if METRIC_START_YEAR <= y <= METRIC_END_YEAR}
        | {y for y in trials_by_year if METRIC_START_YEAR <= y <= METRIC_END_YEAR}
    )

    for year in years:
        trials = trials_by_year.get(year, [])
        counts = Counter(t["phase_assigned"] for t in trials)
        total = len(trials)
        inferred = sum(1 for t in trials if t.get("date_inferred"))
        pct_inferred = (inferred / total) if total else 0.0

        wout = weighted_output(counts)
        phased = total - counts.get("NA", 0)
        late = counts.get("PHASE3", 0) + counts.get("PHASE4", 0)

        rd_here = rd_by_year.get(year)
        row: Dict[str, Any] = {
            "company_id": company_id,
            "year": year,
            "rd_expense_usd": rd_here["value"] if rd_here else None,
            "rd_definition": rd_definition_for_tag(rd_here["tag"]) if rd_here else None,
            "rd_is_spliced": bool(rd_here.get("spliced")) if rd_here else False,
            "trials_started_total": total,
            "trials_early_p1": counts.get("EARLY_PHASE1", 0),
            "trials_p1": counts.get("PHASE1", 0),
            "trials_p2": counts.get("PHASE2", 0),
            "trials_p3": counts.get("PHASE3", 0),
            "trials_p4": counts.get("PHASE4", 0),
            "trials_na": counts.get("NA", 0),
            "weighted_output": round(wout, 2),
            "late_stage_share": round(late / phased, 4) if phased else None,
            "pct_dates_inferred": round(pct_inferred, 4),
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }

        spliced_any = False
        for lag in LAGS:
            key = f"output_per_musd_lag{lag}"
            src = rd_by_year.get(year - lag)
            # The ratio is suppressed below the trial threshold. A ratio built
            # on one or two trials is noise wearing the costume of a metric.
            if not src or not src.get("value") or src["value"] <= 0 \
               or total < MIN_TRIALS_FOR_RATIO:
                row[key] = None
                continue
            if src.get("spliced"):
                spliced_any = True
            row[key] = round(wout / (src["value"] / 1_000_000), 6)

        row["confidence"] = rate_confidence(
            total, pct_inferred, spliced_any or row["rd_is_spliced"]
        )
        rows.append(row)
    return rows


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main() -> int:
    started = time.time()
    try:
        ua = require_env("SEC_USER_AGENT", must_contain="@")
        sb_url = require_env("SUPABASE_URL", must_contain="supabase")
        sb_key = require_env("SUPABASE_SERVICE_ROLE_KEY")
    except EtlError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return 1

    sb = Supabase(sb_url, sb_key)
    errors: List[Dict[str, str]] = []

    try:
        run = sb.insert_one("ingestion_runs", {"job_name": "full_ingest",
                                               "status": "running"})
        run_id = run["id"]
    except EtlError as exc:
        print(f"FATAL: could not start run: {exc}", file=sys.stderr)
        return 1

    def log_error(source: str, key: str, err_type: str, message: str) -> None:
        errors.append({"run_id": run_id, "source": source, "entity_key": key[:200],
                       "error_type": err_type, "message": str(message)[:1000]})
        print(f"  ERROR [{source}] {key}: {message}", file=sys.stderr)

    records_in = 0
    records_upserted = 0

    try:
        companies = sb.select("companies", {"select": "id,cik,ticker,name",
                                            "in_scope": "eq.true"})
        aliases = sb.select("sponsor_aliases",
                            {"select": "id,company_id,sponsor_name,effective_from,"
                                       "effective_to,status"})
        print(f"Loaded {len(companies)} companies, {len(aliases)} aliases\n")

        by_id = {c["id"]: c for c in companies}

        # ---- SEC --------------------------------------------------------
        print("=== SEC financials ===")
        sec_headers = {"User-Agent": ua, "Accept-Encoding": "gzip, deflate"}
        rd_series: Dict[str, Dict[int, Dict[str, Any]]] = {}
        fin_rows: List[Dict[str, Any]] = []

        for c in companies:
            try:
                facts = http_get_json(
                    f"https://data.sec.gov/api/xbrl/companyfacts/CIK{c['cik']}.json",
                    sec_headers, throttle=SEC_THROTTLE,
                    label=f"companyfacts {c['ticker']}")
                if not facts:
                    log_error("sec", c["ticker"], "no_facts", "companyfacts returned nothing")
                    continue
                series = build_rd_series(facts, c["ticker"])
                if not series:
                    log_error("sec", c["ticker"], "no_rd_tag", "no usable R&D tag")
                    continue
                rd_series[c["id"]] = series
                records_in += len(series)
                for year, rec in series.items():
                    fin_rows.append({
                        "company_id": c["id"],
                        "fiscal_year": year,
                        "period_end": rec["period_end"].isoformat(),
                        "xbrl_tag": rec["tag"],
                        "rd_definition": rd_definition_for_tag(rec["tag"]),
                        "value_usd": rec["value"],
                        "form": "10-K",
                        "filed_date": rec["filed"].isoformat()
                                      if rec["filed"] != date.min else None,
                        "accession": rec.get("accession"),
                        "is_restated": bool(rec.get("restated")),
                        "is_spliced_year": bool(rec.get("spliced")),
                    })
                span = f"{min(series)}-{max(series)}"
                print(f"  {c['ticker']:5s} {len(series):2d} years ({span})")
            except EtlError as exc:
                log_error("sec", c["ticker"], "fetch_failed", exc)

        records_upserted += sb.upsert("financial_facts", fin_rows,
                                      "company_id,fiscal_year")
        print(f"  upserted {len(fin_rows)} financial facts\n")

        # ---- ClinicalTrials.gov, one query PER ALIAS --------------------
        print("=== ClinicalTrials.gov (one query per alias) ===")
        all_trials: Dict[str, Dict[str, Any]] = {}
        alias_hits: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        for a in aliases:
            if a["status"] == "divested" and a.get("effective_to") is None:
                continue
            try:
                found = fetch_trials_for_sponsor(a["sponsor_name"], ua)
                records_in += len(found)
                for t in found:
                    all_trials[t["nct_id"]] = t
                    alias_hits[a["id"]].append(t)
                label = a["sponsor_name"][:52]
                print(f"  {label:54s} {len(found):5d}")
            except EtlError as exc:
                log_error("ctgov", a["sponsor_name"], "fetch_failed", exc)

        trial_rows = list(all_trials.values())
        records_upserted += sb.upsert("trials", trial_rows, "nct_id")
        print(f"  upserted {len(trial_rows)} distinct trials\n")

        # ---- Map trials to companies with effective dating --------------
        print("=== Mapping (effective-date aware) ===")
        map_rows: List[Dict[str, str]] = []
        seen: set = set()
        trials_by_company: Dict[str, Dict[int, List[Dict[str, Any]]]] = \
            defaultdict(lambda: defaultdict(list))
        skipped_out_of_window = 0

        for a in aliases:
            eff_from = parse_iso(a.get("effective_from"))
            eff_to = parse_iso(a.get("effective_to"))
            for t in alias_hits.get(a["id"], []):
                start = parse_iso(t["start_date"]) if t["start_date"] else None
                if not alias_covers(start, eff_from, eff_to):
                    skipped_out_of_window += 1
                    continue
                key = (t["nct_id"], a["company_id"])
                if key in seen:
                    continue
                seen.add(key)
                map_rows.append({"nct_id": t["nct_id"],
                                 "company_id": a["company_id"],
                                 "alias_id": a["id"]})
                if start:
                    trials_by_company[a["company_id"]][start.year].append(t)

        records_upserted += sb.upsert("trial_company_map", map_rows,
                                      "nct_id,company_id")
        print(f"  mapped {len(map_rows)} trial-company pairs")
        print(f"  excluded {skipped_out_of_window} outside their alias's "
              f"effective window\n")

        # ---- Metrics ----------------------------------------------------
        print("=== Metrics ===")
        metric_rows: List[Dict[str, Any]] = []
        for cid, series in rd_series.items():
            metric_rows.extend(
                compute_metrics(cid, series, trials_by_company.get(cid, {}))
            )
        records_upserted += sb.upsert("company_year_metrics", metric_rows,
                                      "company_id,year")

        suppressed = sum(1 for r in metric_rows if r["output_per_musd_lag2"] is None)
        low_conf = sum(1 for r in metric_rows if r["confidence"] == "low")
        print(f"  computed {len(metric_rows)} company-year rows")
        print(f"  {suppressed} had the lag-2 ratio suppressed "
              f"(<{MIN_TRIALS_FOR_RATIO} trials or no lagged R&D)")
        print(f"  {low_conf} rated low confidence\n")

        for cid, c in by_id.items():
            rows = [r for r in metric_rows if r["company_id"] == cid]
            usable = [r for r in rows if r["output_per_musd_lag2"] is not None]
            print(f"  {c['ticker']:5s} {len(rows):3d} years, {len(usable):3d} usable")

        status = "completed_with_errors" if errors else "completed"

    except Exception as exc:  # noqa: BLE001 - the run must always be closed out
        log_error("pipeline", "main", type(exc).__name__, exc)
        status = "failed"

    if errors:
        try:
            sb.upsert("ingestion_errors", errors, "id")
        except EtlError as exc:
            print(f"  could not record errors: {exc}", file=sys.stderr)

    sb.patch("ingestion_runs", {"id": run_id}, {
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "records_in": records_in,
        "records_upserted": records_upserted,
        "errors_count": len(errors),
        "notes": f"Completed in {time.time() - started:.0f}s",
    })

    print(f"\n=== {status.upper()} in {time.time() - started:.0f}s ===")
    print(f"records in: {records_in}, upserted: {records_upserted}, "
          f"errors: {len(errors)}")
    return 0 if status != "failed" else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except EtlError as exc:
        print(f"\nFATAL: {exc}", file=sys.stderr)
        sys.exit(1)
