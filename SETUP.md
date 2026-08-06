# PharmaPulse — Phase 0 Setup

No local installs. No terminal. Everything happens in the browser.
Roughly 10 minutes of clicking, then ~5 minutes of waiting while it runs.

---

## 1. Create the GitHub repository

1. Go to https://github.com/new
2. Repository name: `pharmapulse`
3. Set it to **Private**
4. Do **not** tick "Add a README file"
5. Click **Create repository**

---

## 2. Upload the files

On the empty repo page, click the **uploading an existing file** link.

Drag in these four files, keeping the folder structure:

```
requirements.txt
.gitignore
scripts/phase0_spike.py
.github/workflows/phase0-spike.yml
```

**Important:** the browser upload preserves folders if you drag the whole
`pharmapulse-phase0` folder in at once. Do that rather than dragging files
individually, otherwise `scripts/` and `.github/workflows/` will be flattened
and the workflow will not be detected.

Commit message: `Phase 0 feasibility spike`

Click **Commit changes**.

---

## 3. Add the SEC contact secret

The SEC rejects API requests that do not identify the requester. This is their
published requirement, not an optional courtesy.

1. In your repo, go to **Settings** (top nav of the repo, not your account)
2. Left sidebar: **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Name: `SEC_USER_AGENT`
5. Secret value: `PharmaPulse Research your.email@example.com`
   — replace with your real email address. It must contain an `@`.
6. Click **Add secret**

---

## 4. Run it

1. Go to the **Actions** tab
2. If you see a "Workflows aren't being run on this forked repository" banner,
   click the green **I understand my workflows, go ahead and enable them** button
3. Left sidebar: click **Phase 0 Feasibility Spike**
4. On the right: **Run workflow** → **Run workflow**
5. Wait. The run takes roughly 3-6 minutes, most of it deliberate rate-limit
   throttling against ClinicalTrials.gov.

---

## 5. Get the report

**Option A (easier):** click the running job → click the **Run the feasibility
spike** step → the full report prints in the log. Select all and copy.

**Option B:** on the completed run's summary page, scroll to **Artifacts** and
download `phase0-report`. It contains `phase0_report.md`.

---

## 6. Paste the report back into the chat

Paste the whole thing. Do not skip the tables — the sponsor-name table in Q2 is
the seed data for the next step, and the verdicts determine whether the company
universe needs to shrink before the database is designed.

---

## If something fails

| Symptom | Cause | Fix |
|---|---|---|
| `FATAL: SEC_USER_AGENT is missing` | Secret not set, or has no `@` | Redo step 3 |
| `HTTP 403` from SEC | Rate limit or rejected User-Agent | Wait 10 minutes, re-run |
| Workflow doesn't appear in Actions | `.github/workflows/` folder was flattened on upload | Re-upload with folder structure intact |
| Run times out at 30 minutes | ClinicalTrials.gov is slow or throttling | Re-run; if it repeats, paste the log and I'll reduce the page cap |

Paste any error output into the chat verbatim and I'll generate a corrective fix.
