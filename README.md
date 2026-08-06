# PharmaPulse

Analytics platform joining SEC EDGAR R&D financial disclosures to
ClinicalTrials.gov pipeline activity, to answer one question:

> How efficiently do pharmaceutical companies convert R&D spending into
> clinical trial activity?

**Status: Phase 0 — feasibility spike.** No application code yet, by design.

---

## What this repository currently contains

A single diagnostic script that runs in GitHub Actions and answers three
questions before any database or UI is built:

1. **Does the XBRL tag `ResearchAndDevelopmentExpense` exist** for the target
   companies, with usable annual 10-K values across ~10 years?
2. **What are the real ClinicalTrials.gov lead-sponsor name variants** per
   company? There is no shared identifier between the two data sources, so this
   join must be curated by hand. This output is the seed data for it.
3. **Does the capital-efficiency metric produce a useful spread** across
   companies, or does everyone land in the same narrow band?

If question 2 or 3 fails, the product scope changes before anything is built.
That is the entire point of running this first.

---

## Structure

```
.
├── .github/
│   └── workflows/
│       └── phase0-spike.yml     GitHub Actions workflow (manual trigger)
├── scripts/
│   └── phase0_spike.py          The diagnostic script
├── requirements.txt             Python dependencies
├── .gitignore
├── SETUP.md                     Click-by-click setup instructions
└── README.md                    This file
```

---

## Getting started

Follow **[SETUP.md](SETUP.md)**. No local Python install and no terminal are
required — everything runs in GitHub Actions and in the browser.

Short version:

1. Create a private GitHub repo
2. Upload these files, preserving the folder structure
3. Add a repository secret named `SEC_USER_AGENT` containing your name and email
4. Actions tab → **Phase 0 Feasibility Spike** → **Run workflow**
5. Read the report in the run log, or download the `phase0-report` artifact

---

## Uploading: two options

**Option A — drag and drop (fastest).**
Extract the ZIP and drag the *whole folder* onto GitHub's upload page. Do not
drag files one at a time, or the `scripts/` and `.github/workflows/` folders
will be flattened and the workflow will never be detected.

> **If the workflow does not appear in the Actions tab after uploading,** the
> `.github` folder was almost certainly dropped. Some browsers and macOS Finder
> hide folders beginning with a dot. Use Option B for that one file.

**Option B — create the file directly on GitHub (most reliable).**
On the repo page click **Add file → Create new file**, then type the full path
into the filename box, including slashes:

```
.github/workflows/phase0-spike.yml
```

GitHub creates the folders automatically as you type each `/`. Paste the file
contents, then commit. Repeat for `scripts/phase0_spike.py` if needed.

---

## Data sources

| Source | Use | Access |
|---|---|---|
| SEC EDGAR XBRL (`data.sec.gov`) | Annual R&D expense from 10-K filings | Public, no key. A descriptive `User-Agent` with contact info is required |
| ClinicalTrials.gov API v2 | Interventional, industry-sponsored trial records | Public, no key |

Both are US Government works. The script throttles well below the published
rate limits and retries with exponential backoff.

---

## Notes and limitations

- Reported R&D expense is **company-wide and unallocated**. It includes
  discovery, preclinical, platform work, and terminated programs, none of which
  produce a registered trial. Any ratio built on it is descriptive, not causal.
- R&D spending in year *Y* funds trials starting roughly in years *Y+2* to
  *Y+5*. The spike uses a 2-year lag by default; this is an assumption.
- Phase weights (P1=1, P2=3, P3=9, P4=2) reflect rough relative trial cost.
  **They are assumptions, not measured values.**
- Foreign private issuers filing 20-F instead of 10-K are out of scope, which
  excludes several large European pharma companies.

---

## Not investment advice

This project analyses public data for research and portfolio purposes. It does
not produce investment recommendations. The SEC and ClinicalTrials.gov are not
affiliated with it.
