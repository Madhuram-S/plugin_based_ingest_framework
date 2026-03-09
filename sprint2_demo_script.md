# Sprint 2 Demo — Talk Track

> **Audience:** Mixed (technical + business)
> **Target runtime:** Under 4 minutes
> **Format:** Ready-to-read talk track with demo cues

---

## Full Script (~3 min 30 sec)

---

### 🔷 Opening & Context *(~20 sec)*

Hey everyone — thanks for joining. Quick Sprint 2 demo today.
Just to set the stage: in Sprint 1, we connected to our data sources — Epic and a set of side-loaded files.
This sprint, we built on top of that. Three user stories, all focused on **hardening the bronze layer** — bronze just meaning our raw landing zone where data first arrives — and standing up our first round of **data quality checks**.

Let me walk you through each one.

---

### 🔷 1. Orchestration Pipeline *(~50 sec)*

First up — the ingestion pipeline. Think of this as the conductor that kicks off and manages the whole data movement process.

> **[show pipeline run]**

What we built here is a pipeline that can run the ingestion for each source and each object **in parallel** — meaning they don't have to wait for each other.
For example, Epic's patient data, encounter data, and scheduling data can all load at the same time, because each one lands in its own independent target table.

For the business side: this directly shortens our pipeline runtime, so data lands faster and we're not sitting on a long sequential queue.
For the technical folks: we're spinning up parallel Databricks jobs per object, scoped to each source-object combination.

---

### 🔷 2. CDC Query Layer *(~45 sec)*

Next — we built out the SQL queries that pull the **latest changes** from our **Change Data Capture (CDC)** tables for Epic.
CDC — or Change Data Capture — is the mechanism that tracks every insert, update, or delete at the source system, so we only pull what's new instead of re-loading everything each run.

> **[show CDC query output / sample result set]**

These queries are designed to grab the most recent record for each row based on the CDC timestamp.
This keeps our bronze layer in sync with Epic without re-processing the entire history on every run — which is both more efficient and easier to audit.

---

### 🔷 3. Data Quality (DQ) and Security Checks *(~1 min)*

Last piece — and probably the most visible one going forward — **data quality checks**.
We kept this sprint's scope focused: minimum row count checks, null checks, and duplicate checks.

> **[show DQ results table]**

The intentional design here: bronze is meant to be a **mirror of the source** — we don't drop records, even if they fail a check. What we *do* is flag them and log the results.

Every check result gets written to a tracking table — so we know:
- Which run it came from
- Which specific check failed
- How many records were affected

> **[show alert / notification setup]**

We also wired up **alerts**. If a check fails, the run is tagged as failed and the team gets notified right away. If it's a warning — something worth watching but not a blocker — we send a less frequent email digest so we're not flooding anyone's inbox.

The business value here: we now have visibility and traceability into data health from day one, without throwing away records before we've had a chance to investigate.

---

### 🔷 Closing *(~15 sec)*

That's the Sprint 2 wrap-up — orchestration pipeline, CDC queries, and data quality groundwork all in place.
Happy to go deeper on any of these, or if there are questions from either side of the table, let's dig in now.

**Any questions?**

---
---

## ⚡ 60-Second Backup Version

Hey everyone — quick Sprint 2 update.

We shipped three things this sprint.
**One:** An orchestration pipeline that ingests data from all our sources in parallel — so things run faster and independently.
**Two:** CDC (Change Data Capture) queries for Epic — meaning we only pull new or changed records each run, not the full history.
**Three:** Data quality checks — row count, nulls, duplicates. Bronze — our raw landing zone — stays as a true mirror of the source; we log failures instead of dropping records, and the team gets alerted automatically when something's off.

Foundation's solid. Happy to take any questions.
