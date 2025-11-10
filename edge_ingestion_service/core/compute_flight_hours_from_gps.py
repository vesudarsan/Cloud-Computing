#!/usr/bin/env python3
import os, math
from datetime import datetime, timedelta, timezone
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# ---------------- CONFIG ----------------
INFLUX_URL   = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "UGFYphuRbH-x-KTWrHfiQNbNSH2Wg1R9KPHkp0Ga8WRlbkGTMB2-w2-e8J9xKDMQgotVdhLEHVr82Ll9W7VXvw==")  # avoid committing this
INFLUX_ORG   = os.getenv("INFLUX_ORG", "5ea549634c8f37ac")

BUCKET       = os.getenv("IN_BUCKET", "drone_telemetry")
OUT_BUCKET   = os.getenv("OUT_BUCKET", BUCKET)  # write back to same bucket by default

MEAS_GPOS    = os.getenv("MEAS_GPOS", "GLOBAL_POSITION_INT")
DRONE_TAG    = os.getenv("DRONE_TAG", "drone_id")        # must be a TAG KEY (e.g., drone_id / system_id)
DRONE_VALUE  = os.getenv("DRONE_VALUE") or "123456789"   # optional; filter one drone

# Fields (match your writer)
FIELD_RELALT = os.getenv("FIELD_RELALT", "relative_alt")  # mm
FIELD_VX     = os.getenv("FIELD_VX", "vx")                # cm/s
FIELD_VY     = os.getenv("FIELD_VY", "vy")                # cm/s

# Window (defaults shown; normally set by env from your scheduler)
WINDOW_START = os.getenv("WINDOW_START", "2025-11-04T00:00:00Z")
WINDOW_END   = os.getenv("WINDOW_END",   "2025-11-07T00:00:00Z")
print(f"WINDOW_START={WINDOW_START}, WINDOW_END={WINDOW_END}")
START = datetime.fromisoformat(WINDOW_START.replace("Z","+00:00"))
END   = datetime.fromisoformat(WINDOW_END.replace("Z","+00:00"))

# Flight detection thresholds (tune via env)
ALT_START_M   = float(os.getenv("ALT_START_M", "1.5"))
ALT_END_M     = float(os.getenv("ALT_END_M",   "0.8"))
SPD_START_MPS = float(os.getenv("SPD_START_MPS", "0.8"))
SPD_END_MPS   = float(os.getenv("SPD_END_MPS",   "0.3"))
START_HOLD    = int(os.getenv("START_HOLD", "2"))
END_HOLD      = int(os.getenv("END_HOLD",   "3"))
MAX_GAP_SEC   = int(os.getenv("MAX_GAP_SEC", "10"))
MIN_SESSION_SEC = int(os.getenv("MIN_SESSION_SEC", "5"))

# Measurements/fields to write
MEAS_SUMMARY   = os.getenv("MEAS_SUMMARY", "flight_hours")              # per-window
MEAS_SESSION   = os.getenv("MEAS_SESSION", "flight_session")            # per-session (optional)
MEAS_CUMUL     = os.getenv("MEAS_CUMUL", "flight_hours_cumulative")     # running total
FIELD_HOURS    = os.getenv("FIELD_HOURS", "hours")
FIELD_SECONDS  = os.getenv("FIELD_SECONDS", "seconds")

WRITE_SESSIONS = os.getenv("WRITE_SESSIONS", "1") == "1"

# -------------- QUERY -------------------
def build_flux_query():
    # Filter by fields, measurement, window, and (optionally) a specific drone tag value
    tag_filter = f'  |> filter(fn: (r) => r.{DRONE_TAG} == "{DRONE_VALUE}")\n' if DRONE_VALUE else ""
    q = f'''
from(bucket: "{BUCKET}")
  |> range(start: {START.isoformat()}, stop: {END.isoformat()})
  |> filter(fn: (r) => r._measurement == "{MEAS_GPOS}")
  |> filter(fn: (r) => r._field == "{FIELD_RELALT}" or r._field == "{FIELD_VX}" or r._field == "{FIELD_VY}")
{tag_filter}  |> pivot(rowKey: ["_time","{DRONE_TAG}"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time","{DRONE_TAG}","{FIELD_RELALT}","{FIELD_VX}","{FIELD_VY}"])
  |> sort(columns: ["_time"])
'''
    print("Flux Query:\n", q)
    return q

# -------------- CORE: session detection ---------------
def detect_sessions(df: pd.DataFrame):
    if df.empty: return []
    g = df.copy()
    g["_time"] = pd.to_datetime(g["_time"], utc=True)
    g = g.sort_values("_time").reset_index(drop=True)

    ra_mm  = pd.to_numeric(g[FIELD_RELALT], errors="coerce")
    vx_cms = pd.to_numeric(g[FIELD_VX], errors="coerce")
    vy_cms = pd.to_numeric(g[FIELD_VY], errors="coerce")

    ra_m   = (ra_mm.fillna(-1e12) / 1000.0).astype(float)
    gs_mps = (((vx_cms.fillna(0.0))**2 + (vy_cms.fillna(0.0))**2)**0.5 / 100.0).astype(float)

    times = g["_time"].to_numpy()
    sessions = []
    in_flight = False
    start_hold = end_hold = 0
    sess_start = None
    last_t = None

    for i, t in enumerate(times):
        if last_t is not None:
            gap = (t - last_t).total_seconds()
            if in_flight and gap > MAX_GAP_SEC:
                if sess_start is not None:
                    sessions.append((sess_start, last_t, (last_t - sess_start).total_seconds()))
                in_flight = False
                start_hold = end_hold = 0
                sess_start = None

        above_start = (ra_m.iat[i] >= ALT_START_M) or (gs_mps.iat[i] >= SPD_START_MPS)
        below_end   = (ra_m.iat[i] <= ALT_END_M)   and (gs_mps.iat[i] <= SPD_END_MPS)

        if not in_flight:
            if above_start:
                start_hold += 1
                if start_hold >= START_HOLD:
                    in_flight = True
                    sess_start = t
                    end_hold = 0
                    start_hold = 0
            else:
                start_hold = 0
        else:
            if below_end:
                end_hold += 1
                if end_hold >= END_HOLD:
                    if sess_start is not None:
                        dur = (t - sess_start).total_seconds()
                        if dur >= MIN_SESSION_SEC:
                            sessions.append((sess_start, t, dur))
                    in_flight = False
                    sess_start = None
                    start_hold = end_hold = 0
            else:
                end_hold = 0

        last_t = t

    if in_flight and sess_start is not None:
        dur = (times[-1] - sess_start).total_seconds()
        if dur >= MIN_SESSION_SEC:
            sessions.append((sess_start, times[-1], dur))

    return sessions

# -------------- READ existing window & cumulative -----
def get_existing_window_hours(qapi, bucket, drone_tag, drone_id, start, end):
    q = f'''
from(bucket: "{bucket}")
  |> range(start: {start.isoformat()}, stop: {end.isoformat()})
  |> filter(fn: (r) => r._measurement == "{MEAS_SUMMARY}")
  |> filter(fn: (r) => r.{drone_tag} == "{drone_id}")
  |> filter(fn: (r) => r.window_start == "{start.isoformat()}" and r.window_end == "{end.isoformat()}")
  |> filter(fn: (r) => r._field == "{FIELD_HOURS}")
  |> last()
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time","{FIELD_HOURS}"])
'''
    df = qapi.query_data_frame(q)
    if isinstance(df, list) and df:
        df = pd.concat(df, ignore_index=True)
    if df is None or df.empty or FIELD_HOURS not in df:
        return 0.0
    try:
        return float(df[FIELD_HOURS].iloc[0])
    except Exception:
        return 0.0


def get_last_cumulative_hours(qapi, bucket, drone_tag, drone_id, before_time):
    q = f'''
from(bucket: "{bucket}")
  |> range(start: 0, stop: {before_time.isoformat()})
  |> filter(fn: (r) => r._measurement == "{MEAS_CUMUL}")
  |> filter(fn: (r) => r.{drone_tag} == "{drone_id}")
  |> filter(fn: (r) => r._field == "{FIELD_HOURS}")
  |> last()
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time","{FIELD_HOURS}"])
'''
    df = qapi.query_data_frame(q)
    if isinstance(df, list) and df:
        df = pd.concat(df, ignore_index=True)
    if df is None or df.empty or FIELD_HOURS not in df:
        return 0.0
    try:
        return float(df[FIELD_HOURS].iloc[0])
    except Exception:
        return 0.0

# -------------- MAIN --------------------
def main():
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    qapi = client.query_api()
    wapi = client.write_api(write_options=SYNCHRONOUS)

    # Avoid future timestamps for writes
    now_utc = datetime.now(timezone.utc)
    safe_summary_time = min(END, now_utc - timedelta(seconds=1))

    df = qapi.query_data_frame(build_flux_query())
    if isinstance(df, list):
        df = pd.concat(df, ignore_index=True) if df else pd.DataFrame()
    print(f"Queried {0 if df is None else len(df)} rows from InfluxDB.")

    if df is None or df.empty:
        print("No GLOBAL_POSITION_INT data in window.")
        client.close()
        return

    # Ensure tag column exists (client sometimes mangles names)
    if DRONE_TAG not in df.columns:
        guess = next((c for c in df.columns if c.startswith(DRONE_TAG)), None)
        if guess:
            df[DRONE_TAG] = df[guess]
        else:
            # If querying one drone only, inject the value
            if DRONE_VALUE:
                df[DRONE_TAG] = DRONE_VALUE
            else:
                df[DRONE_TAG] = "DRONE"

    df = df.dropna(subset=["_time"]).copy()

    # ---- Detect sessions per drone ----
    results = []     # (drone_id, window_sec, window_hours, sessions)
    for drone_id, g in df.groupby(DRONE_TAG):
        sessions = detect_sessions(g[["_time", FIELD_RELALT, FIELD_VX, FIELD_VY]])
        total_sec = sum(d for _, _, d in sessions)
        total_hours = total_sec / 3600.0
        results.append((str(drone_id), total_sec, total_hours, sessions))

    # ---- Build summary & cumulative points (idempotent) ----
    summary_points = []
    cumulative_points = []
    session_points = []

    for drone_id, window_sec, window_hours, sessions in results:
        # read already-written window hours (if any)
        existed_hours = get_existing_window_hours(qapi, OUT_BUCKET, DRONE_TAG, drone_id, START, END)

        # delta to add to cumulative
        delta_hours = round(window_hours - existed_hours, 6)
        delta_seconds = round(window_sec - existed_hours * 3600.0, 3)

        # read last cumulative BEFORE this window
        base_total_hours = get_last_cumulative_hours(qapi, OUT_BUCKET, DRONE_TAG, drone_id, START)

        # compute new cumulative (allow negatives only if you want retro corrections)
        new_total_hours = round(base_total_hours + max(delta_hours, 0.0), 6)
        new_total_seconds = round(new_total_hours * 3600.0, 3)

        # per-window summary (overwrite for this window)
        sp = (
            Point(MEAS_SUMMARY)
            .tag(DRONE_TAG, drone_id)
            .tag("window_start", START.isoformat())
            .tag("window_end", END.isoformat())
            .field(FIELD_SECONDS, round(window_sec, 3))
            .field(FIELD_HOURS, round(window_hours, 6))
            .time(safe_summary_time, WritePrecision.NS)
        )
        summary_points.append(sp)

        # cumulative total at window end
        cp = (
            Point(MEAS_CUMUL)
            .tag(DRONE_TAG, drone_id)
            .field(FIELD_SECONDS, new_total_seconds)
            .field(FIELD_HOURS, new_total_hours)
            .time(safe_summary_time, WritePrecision.NS)
        )
        cumulative_points.append(cp)

        # optional: write each session row
        if WRITE_SESSIONS and sessions:
            for s_start, s_end, dur in sessions:
                session_points.append(
                    Point(MEAS_SESSION)
                    .tag(DRONE_TAG, drone_id)
                    .tag("start", s_start.isoformat())
                    .tag("end", s_end.isoformat())
                    .field("duration_sec", round(dur, 3))
                    .time(s_end, WritePrecision.NS)
                )

    # ---- Writes (synchronous) ----
    if summary_points:
        wapi.write(bucket=OUT_BUCKET, record=summary_points)
    if cumulative_points:
        wapi.write(bucket=OUT_BUCKET, record=cumulative_points)
    if session_points:
        wapi.write(bucket=OUT_BUCKET, record=session_points)

    # log
    for drone_id, window_sec, window_hours, _ in results:
        print(f"{drone_id}: window={window_hours:.3f} h ({window_sec:.0f} s) | cumulative written at {safe_summary_time.isoformat()}")

    client.close()

if __name__ == "__main__":
    main()
