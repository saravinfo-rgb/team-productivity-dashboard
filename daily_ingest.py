r"""
Daily Productivity Report — Ingestion Automation
==================================================
Purpose:
  Runs once a day (via Windows Task Scheduler) after the NIMBLE report for
  the day has landed on the network share. It finds today's file using the
  known naming/folder pattern, appends it to a single master dataset, and
  (optionally) regenerates the aggregated JSON that powers the dashboard.

Folder pattern (confirmed from the two sample paths given):
  \\workflow\share\NIMBLE\Daily_Productivity_Report\{YYYY}\{Mon}\{YYYY-MM-DD}\Daily\
  productivity_detailed_report_{start_date}_{end_date}.csv
  where {end_date} == the folder's {YYYY-MM-DD} and {start_date} == end_date - 1 day.

Schedule this with Windows Task Scheduler to run daily, e.g. 06:30 AM,
after the report generation job that produces the file is expected to finish.
"""

import os
import shutil
import datetime as dt
import pandas as pd
import json

# ---------------------------------------------------------------------------
# CONFIG — adjust these three paths for your environment
# ---------------------------------------------------------------------------
SHARE_ROOT = r"\\workflow\share\NIMBLE\Daily_Productivity_Report"
MASTER_STORE = r"d:\ProductivityReporting\master_dataset.parquet"   # running history
AGG_OUTPUT = r"d:\ProductivityReporting\agg_data.json"              # raw aggregates (optional/debug)
LOG_FILE = r"d:\ProductivityReporting\ingest_log.txt"

# The dashboard is regenerated fresh each run so it can just be double-clicked
# (no web server needed). Put DASHBOARD_OUTPUT somewhere the account manager
# can reach directly — a shared drive folder, a Teams-synced folder, etc.
DASHBOARD_TEMPLATE = r"d:\ProductivityReporting\dashboard_template.html"
DASHBOARD_OUTPUT = r"d:\ProductivityReporting\Published\Productivity_Dashboard.html"

# Columns we actually need downstream (keeps the master file small)
KEEP_COLS = [
    "UID", "Name", "shift_date", "teamgroup", "department", "Team",
    "Stage", "Process", "taskstatus", "category", "unit", "actual", "target",
    "efficitiveHoursDigits", "totaltime", "non_productivity_hours",
    "Article Id / ISBN", "Customer", "Type", "Shift", "Skill Level"
]


# Columns computed during ingestion (not present in the raw CSV) that
# downstream aggregation depends on. Tracked separately from KEEP_COLS so
# schema-drift detection catches additions to *either* list.
DERIVED_COLS = ["totaltime_seconds", "non_prod_seconds", "article_id_clean", "shift_hours"]


def _totaltime_to_seconds(val):
    """Convert 'HH:MM:SS' totaltime strings to seconds. Non-time values -> 0."""
    try:
        h, m, s = str(val).split(":")
        return int(h) * 3600 + int(m) * 60 + int(s)
    except (ValueError, AttributeError):
        return 0


# Shift codes in the source data are prefixed (e.g. FSDWFH, SSNWFO, NSWFH,
# GSDWFO) — the prefix identifies the shift type:
#   FS = First Shift   -> 7 hours
#   SS = Second Shift   -> 7 hours
#   NS = Night Shift    -> 8 hours
#   GS = General Shift  -> 8 hours
# Codes that don't match any of these (e.g. the rare "SW") are left as None
# and excluded from the productivity-% denominator rather than guessed at.
def _shift_hours(code):
    code = str(code).strip().upper()
    if code.startswith("FS") or code.startswith("SS"):
        return 7.0
    if code.startswith("NS") or code.startswith("GS"):
        return 8.0
    return None


def log(msg: str):
    line = f"{dt.datetime.now().isoformat()}  {msg}"
    print(line)
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def expected_file_for(target_date: dt.date) -> str:
    """Build the expected file path for a given report date, following the
    confirmed folder/filename convention."""
    start_date = target_date - dt.timedelta(days=1)
    year = target_date.strftime("%Y")
    month = target_date.strftime("%b")            # e.g. 'Jul'
    folder_date = target_date.strftime("%Y-%m-%d")
    filename = (
        f"productivity_detailed_report_"
        f"{start_date.strftime('%Y-%m-%d')}_{target_date.strftime('%Y-%m-%d')}.csv"
    )
    return os.path.join(SHARE_ROOT, year, month, folder_date, "Daily", filename)


def load_master() -> pd.DataFrame:
    if os.path.exists(MASTER_STORE):
        existing = pd.read_parquet(MASTER_STORE)
        missing = [c for c in KEEP_COLS + DERIVED_COLS if c not in existing.columns]
        if missing:
            log(f"Schema change detected (missing columns: {missing}). "
                f"Rebuilding master from source files via lookback re-ingest.")
            return pd.DataFrame(columns=KEEP_COLS)
        return existing
    return pd.DataFrame(columns=KEEP_COLS)


def ingest_date(target_date: dt.date, master: pd.DataFrame) -> pd.DataFrame:
    path = expected_file_for(target_date)
    if not os.path.exists(path):
        log(f"SKIP  {target_date}: file not found at {path}")
        return master

    if target_date.strftime("%Y-%m-%d") in set(master.get("shift_date", [])):
        log(f"SKIP  {target_date}: already ingested")
        return master

    df = pd.read_csv(path, low_memory=False)
    df = df[df["shift_date"] != "-"].copy()          # drop non-activity rows
    df = df[[c for c in KEEP_COLS if c in df.columns]]
    df["actual"] = pd.to_numeric(df["actual"], errors="coerce").fillna(0)
    df["target"] = pd.to_numeric(df["target"], errors="coerce").fillna(0)
    df["efficitiveHoursDigits"] = pd.to_numeric(df["efficitiveHoursDigits"], errors="coerce").fillna(0)
    df["totaltime_seconds"] = df["totaltime"].apply(_totaltime_to_seconds) if "totaltime" in df.columns else 0
    df["non_prod_seconds"] = df["non_productivity_hours"].apply(_totaltime_to_seconds) if "non_productivity_hours" in df.columns else 0
    df["shift_hours"] = df["Shift"].apply(_shift_hours) if "Shift" in df.columns else None
    # Article Id / ISBN uses "0" for non-article tasks; treat that as "no article"
    if "Article Id / ISBN" in df.columns:
        df["article_id_clean"] = df["Article Id / ISBN"].astype(str)
        df.loc[df["article_id_clean"].isin(["0", "-", "nan"]), "article_id_clean"] = None
    else:
        df["article_id_clean"] = None

    master = pd.concat([master, df], ignore_index=True) if len(master) else df
    log(f"OK    {target_date}: ingested {len(df):,} rows from {path}")
    return master


def rebuild_aggregates(master: pd.DataFrame):
    """
    Pre-aggregate to keep the dashboard fast and its payload small.
    
    This function creates multiple aggregation tables:
    1. daily - Daily summary metrics
    2. customer - Customer-level metrics by day
    3. employee - Employee-level metrics by day
    4. process_breakdown - Detailed process breakdown with cluster/department/stage
    5. process_daily - NEW: Daily process outflow (no cluster/department grouping)
    6. user_productivity - User-level per process with article counts
    7. user_summary - NEW: Rolled-up user summary across all processes
    """
    
    # --- Daily aggregation ---
    daily = master.groupby("shift_date").agg(
        tasks=("UID", "count"),
        employees=("UID", "nunique"),
        actual=("actual", "sum"),
        target=("target", "sum"),
        eff_hours=("efficitiveHoursDigits", "sum"),
    ).reset_index()
    completed = (
        master[master["taskstatus"] == "Completed"]
        .groupby("shift_date")["UID"].count()
        .reindex(daily["shift_date"]).fillna(0).values
    )
    daily["completed"] = completed

    # --- Customer aggregation ---
    customer = master.groupby(["shift_date", "Customer"]).agg(
        tasks=("UID", "count"), 
        actual=("actual", "sum"),
        target=("target", "sum"), 
        eff_hours=("efficitiveHoursDigits", "sum"),
        employees=("UID", "nunique"),
    ).reset_index()
    completed_by_cust = (
        master[master["taskstatus"] == "Completed"]
        .groupby(["shift_date", "Customer"])["UID"].count()
    )
    customer["completed"] = customer.apply(
        lambda r: completed_by_cust.get((r["shift_date"], r["Customer"]), 0), axis=1
    )

    # --- Employee aggregation ---
    employee = master.groupby(["shift_date", "UID", "Name", "Customer"]).agg(
        tasks=("UID", "count"), 
        actual=("actual", "sum"),
        target=("target", "sum"), 
        eff_hours=("efficitiveHoursDigits", "sum"),
        dept=("department", "first"), 
        cluster=("teamgroup", "first"),
        shift_hours=("shift_hours", "first"),
    ).reset_index()

    # --- Process Breakdown (with cluster, department, stage, customer) ---
    pb = master[(master["department"] != "-") & (master["Process"] != "-")]
    process_breakdown = pb.groupby(
        ["shift_date", "teamgroup", "department", "Stage", "Process", "Customer"]
    ).agg(
        tasks=("UID", "count"),
        actual=("actual", "sum"),
        target=("target", "sum"),
        eff_hours=("efficitiveHoursDigits", "sum"),
        avg_seconds=("totaltime_seconds", "mean"),
    ).reset_index()
    completed_by_pb = (
        pb[pb["taskstatus"] == "Completed"]
        .groupby(["shift_date", "teamgroup", "department", "Stage", "Process", "Customer"])["UID"]
        .count()
    )
    process_breakdown["completed"] = process_breakdown.apply(
        lambda r: completed_by_pb.get(
            (r["shift_date"], r["teamgroup"], r["department"], r["Stage"], r["Process"], r["Customer"]), 0
        ), axis=1
    )

    # ===== NEW: Process Wise Daily Outflow =====
    # Aggregates process outflow by day (no cluster/department grouping)
    # This gives a clean daily view of what processes are flowing out each day
    process_daily = pb.groupby(
        ["shift_date", "Process"]
    ).agg(
        tasks=("UID", "count"),
        completed=("taskstatus", lambda x: (x == "Completed").sum()),
        actual=("actual", "sum"),
        target=("target", "sum"),
        eff_hours=("efficitiveHoursDigits", "sum"),
        avg_touchtime_seconds=("totaltime_seconds", "mean"),
    ).reset_index()
    
    # Add department and stage info as separate fields for reference
    # We take the most common department/stage per process-day
    dept_info = pb.groupby(["shift_date", "Process"])["department"].agg(
        lambda x: x.mode()[0] if len(x) > 0 else "-"
    )
    stage_info = pb.groupby(["shift_date", "Process"])["Stage"].agg(
        lambda x: x.mode()[0] if len(x) > 0 else "-"
    )
    process_daily = process_daily.merge(
        dept_info.reset_index().rename(columns={"department": "primary_department"}),
        on=["shift_date", "Process"], how="left"
    )
    process_daily = process_daily.merge(
        stage_info.reset_index().rename(columns={"Stage": "primary_stage"}),
        on=["shift_date", "Process"], how="left"
    )
    # Also include customer breakdown count
    customer_count = pb.groupby(["shift_date", "Process"])["Customer"].nunique().reset_index().rename(
        columns={"Customer": "customer_count"}
    )
    process_daily = process_daily.merge(customer_count, on=["shift_date", "Process"], how="left")

    # ===== MODIFIED: User Productivity (with AVG non-prod hours) =====
    # This groups by shift_date, teamgroup, department, UID, Name, Process, Customer
    # Changes: non_prod_seconds is now AVG (not SUM) per the requirement
    up = master[(master["department"] != "-") & (master["Process"] != "-")]
    user_productivity = up.groupby(
        ["shift_date", "teamgroup", "department", "UID", "Name", "Process", "Customer"]
    ).agg(
        tasks=("UID", "count"),
        # ===== KEY: Distinct articles based on NIMBLE manager =====
        # article_id_clean excludes the "0" placeholder for non-article tasks
        articles=("article_id_clean", "nunique"),  
        total_touchtime_seconds=("totaltime_seconds", "sum"),
        eff_hours=("efficitiveHoursDigits", "sum"),
        # ===== KEY CHANGE: non_prod_seconds now AVG, not SUM =====
        non_prod_seconds=("non_prod_seconds", "mean"),  # Average non-productivity hours per row
    ).reset_index()

    # ===== NEW: User Summary (without customer/process dimension) =====
    # For a simpler user-level view - aggregates across all processes/customers
    # Non-prod hours remains as AVG at this level too
    user_summary = up.groupby(
        ["shift_date", "teamgroup", "department", "UID", "Name"]
    ).agg(
        tasks=("UID", "count"),
        articles=("article_id_clean", "nunique"),  # Distinct articles per user per day
        total_touchtime_seconds=("totaltime_seconds", "sum"),
        eff_hours=("efficitiveHoursDigits", "sum"),
        non_prod_seconds=("non_prod_seconds", "mean"),  # AVG remains
        processes=("Process", "nunique"),
        customers=("Customer", "nunique"),
    ).reset_index()

    # --- Build payload with all datasets ---
    payload = {
        "daily": daily.to_dict(orient="records"),
        "customer": customer.to_dict(orient="records"),
        "employee": employee.to_dict(orient="records"),
        "process_breakdown": process_breakdown.to_dict(orient="records"),
        "process_daily": process_daily.to_dict(orient="records"),  # NEW: daily process outflow
        "user_productivity": user_productivity.to_dict(orient="records"),
        "user_summary": user_summary.to_dict(orient="records"),  # NEW: user summary view
        "generated_at": dt.datetime.now().isoformat(),
    }
    
    os.makedirs(os.path.dirname(AGG_OUTPUT), exist_ok=True)
    with open(AGG_OUTPUT, "w") as f:
        json.dump(payload, f, default=str)
    
    log(f"Rebuilt aggregates -> {AGG_OUTPUT} ({len(master):,} total rows in master)")
    log(f"  daily={len(daily)} customer={len(customer)} employee={len(employee)} "
        f"process_breakdown={len(process_breakdown)} process_daily={len(process_daily)} "
        f"user_productivity={len(user_productivity)} user_summary={len(user_summary)}")
    return payload


def publish_dashboard(payload: dict):
    """Inject fresh data into the dashboard template and write the final
    HTML file that the account manager opens directly (double-click, no
    server required)."""
    if not os.path.exists(DASHBOARD_TEMPLATE):
        log(f"WARN  dashboard template not found at {DASHBOARD_TEMPLATE}, skipping publish")
        return
    template = open(DASHBOARD_TEMPLATE, "r", encoding="utf-8").read()
    final_html = template.replace("__REPORT_DATA_JSON__", json.dumps(payload, default=str))
    os.makedirs(os.path.dirname(DASHBOARD_OUTPUT), exist_ok=True)
    with open(DASHBOARD_OUTPUT, "w", encoding="utf-8") as f:
        f.write(final_html)
    log(f"Published dashboard -> {DASHBOARD_OUTPUT}")


def main():
    today = dt.date.today()
    master = load_master()

    # Catch up on the last N days in case the task didn't run for a while
    # (safe no-op for days already ingested or not yet available).
    LOOKBACK_DAYS = 3
    for i in range(LOOKBACK_DAYS, -1, -1):
        master = ingest_date(today - dt.timedelta(days=i), master)

    os.makedirs(os.path.dirname(MASTER_STORE), exist_ok=True)
    master.to_parquet(MASTER_STORE, index=False)

    payload = rebuild_aggregates(master)
    publish_dashboard(payload)


if __name__ == "__main__":
    main()