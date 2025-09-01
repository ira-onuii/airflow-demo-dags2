# -*- coding: utf-8 -*-
"""
KM 파이프라인(주간):
- 매주 일요일 23:59 (Asia/Seoul) 기준
1) new_lecture → lvt_log (INSERT: 열린 episode 없을 때만)
2) pause_lecture → lvt_log (UPDATE: 열린 episode만 end_date 세팅)
3) weekly_actuals 집계/업서트 (월~일)
4) KM 재학습 (as-of = 일요일, lvt_log 기반 검열 포함)
5) model_versions 활성화 롤오버(같은 창/단위 upsert)

연결: postgres_conn_2.0
"""

from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import numpy as np

# -----------------------------
# 설정
# -----------------------------
SEOUL = pendulum.timezone("Asia/Seoul")
CONN_ID = "postgres_conn_2.0"

# 재학습 창: 최근 12개월(필요시 바꾸세요)
FIT_MONTHS = 12
# KM horizon (주) — 필요시 바꾸세요
HORIZON_WEEKS = 52

# -----------------------------
# 유틸
# -----------------------------
def _week_bounds_kst(now_ts: pendulum.DateTime):
    """해당 주 월~일까지 범위(날짜 문자열)"""
    week_end = now_ts.end_of("week")     # 일요일 23:59:59
    week_start = week_end.start_of("week")  # 월요일 00:00:00
    return week_start.date().isoformat(), week_end.date().isoformat()

def _ensure_weekly_actuals(hook: PostgresHook):
    sql = """
    CREATE TABLE IF NOT EXISTS weekly_actuals (
      week_start date PRIMARY KEY,
      new_actual integer NOT NULL DEFAULT 0,
      pause_actual integer NOT NULL DEFAULT 0,
      source varchar(32),
      closed_at timestamptz
    );
    """
    hook.run(sql)

def _ensure_km_tables(hook: PostgresHook):
    sqls = [
        # km_models
        """
        CREATE TABLE IF NOT EXISTS km_models (
          fit_window_start date,
          fit_window_end date,
          time_unit varchar(16),
          cohort_months integer,
          time_k integer,
          s double precision,
          q double precision,
          h double precision
        );
        """,
        # model_versions
        """
        CREATE TABLE IF NOT EXISTS model_versions (
          fit_window_start date,
          fit_window_end date,
          time_unit varchar(16),
          horizon_weeks integer,
          status varchar(16),
          created_at timestamptz DEFAULT now(),
          created_by varchar(64),
          notes text,
          PRIMARY KEY (fit_window_start, fit_window_end, time_unit)
        );
        """
    ]
    for s in sqls:
        hook.run(s)

def _monday(d: pd.Timestamp) -> pd.Timestamp:
    return d - pd.Timedelta(days=d.weekday())

# -----------------------------
# DAG
# -----------------------------
@dag(
    dag_id="km_weekly_full",
    schedule="59 23 * * SUN",  # 매주 일요일 23:59
    start_date=datetime(2025, 1, 1, tzinfo=SEOUL),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["KM_calculator", "weekly", "lvt_log"],
)
def km_weekly_full():

    @task()
    def compute_window():
        now_kst = pendulum.now(SEOUL)
        week_start, week_end = _week_bounds_kst(now_kst)
        # 재학습 창: 최근 FIT_MONTHS개월 (월요일 정렬)
        fit_end = pd.to_datetime(week_end)
        fit_start = (fit_end - pd.DateOffset(months=FIT_MONTHS)).normalize()
        fit_start = _monday(fit_start)  # 창 시작을 월요일로 정렬(선택)
        return {
            "week_start": week_start,   # 월요일
            "week_end": week_end,       # 일요일
            "fit_start": fit_start.date().isoformat(),
            "fit_end": week_end,        # as-of와 동일 주 종료일
            "horizon_weeks": HORIZON_WEEKS,
        }

    @task()
    def insert_new(window: dict):
        """
        new_lecture → lvt_log INSERT
        - 이번 주(start_date ∈ [week_start, week_end]) 신규만
        - 같은 lecture_vt_no에 열린 episode가 없을 때만 새 episode_no로 INSERT
        - tutoring_state는 new_lecture에서 그대로(없으면 NULL)
        - idempotent: 동일 (lecture_vt_no, episode_no, start_date) 있으면 건너뜀
        """
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        sql = """
        WITH params AS (
          SELECT %(week_start)s::date AS week_start, %(week_end)s::date AS week_end
        ),
        new_src AS (
          SELECT
            n.lecture_vt_no,
            n.student_user_no,
            n.fst_months,
            n.start_date::date AS start_date,
            n.tutoring_state
          FROM kpis.new_lecture n, params p
          WHERE n.start_date::date BETWEEN p.week_start AND p.week_end
        ),
        state AS (
          SELECT
            l.lecture_vt_no,
            MAX(l.episode_no) AS max_ep,
            BOOL_OR(l.end_date IS NULL) AS has_open
          FROM kpis.lvt_log l
          GROUP BY l.lecture_vt_no
        ),
        cand AS (
          SELECT
            n.lecture_vt_no,
            n.student_user_no,
            n.fst_months,
            n.start_date,
            n.tutoring_state,
            COALESCE(s.max_ep, 0) + 1 AS next_ep,
            COALESCE(s.has_open, FALSE) AS has_open
          FROM new_src n
          LEFT JOIN state s ON s.lecture_vt_no = n.lecture_vt_no
        )
        INSERT INTO kpis.lvt_log (
          lecture_vt_no, episode_no, student_user_no, fst_months,
          start_date, end_date, tutoring_state, created_at, updated_at
        )
        SELECT
          c.lecture_vt_no,
          c.next_ep AS episode_no,
          c.student_user_no,
          c.fst_months,
          c.start_date,
          NULL AS end_date,
          c.tutoring_state,
          NOW(), NOW()
        FROM cand c
        WHERE c.has_open = FALSE
          AND NOT EXISTS (
            SELECT 1
            FROM kpis.lvt_log x
            WHERE x.lecture_vt_no = c.lecture_vt_no
              AND x.episode_no    = c.next_ep
              AND x.start_date    = c.start_date
          );
        """
        hook.run(sql, parameters=window)
        # 통계
        cnt_sql = """
        WITH p AS (SELECT %(week_start)s::date AS ws, %(week_end)s::date AS we)
        SELECT COUNT(*) FROM kpis.lvt_log, p WHERE start_date BETWEEN p.ws AND p.we;
        """
        cnt = hook.get_first(cnt_sql, parameters=window)[0]
        return {"inserted_new": int(cnt)}

    @task()
    def update_pause(window: dict):
        """
        pause_lecture → lvt_log UPDATE
        - 이번 주(end_date ∈ [week_start, week_end]) 중단만
        - 열린 episode(end_date IS NULL)만 닫음
        - start_date보다 앞설 수 없도록 GREATEST
        """
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        sql = """
        WITH params AS (
          SELECT %(week_start)s::date AS week_start, %(week_end)s::date AS week_end
        ),
        pause_src AS (
          SELECT
            p.lecture_vt_no,
            p.end_date::date AS pause_date,
            p.tutoring_state
          FROM kpis.pause_lecture p, params par
          WHERE p.end_date::date BETWEEN par.week_start AND par.week_end
        )
        UPDATE lvt_log l
        SET
          end_date       = GREATEST(p.pause_date, l.start_date),
          tutoring_state = COALESCE(p.tutoring_state, l.tutoring_state),
          updated_at     = NOW()
        FROM pause_src p
        WHERE p.lecture_vt_no = l.lecture_vt_no
          AND l.end_date IS NULL;
        """
        hook.run(sql, parameters=window)
        # 통계
        cnt_sql = """
        WITH p AS (SELECT %(week_start)s::date AS ws, %(week_end)s::date AS we)
        SELECT COUNT(*) FROM kpis.lvt_log, p WHERE end_date BETWEEN p.ws AND p.we;
        """
        cnt = hook.get_first(cnt_sql, parameters=window)[0]
        return {"updated_pause": int(cnt)}

    @task()
    def aggregate_weekly_actuals(window: dict):
        """
        lvt_log → weekly_actuals 집계/업서트
        - 기준: 월~일
        """
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        _ensure_weekly_actuals(hook)
        sql = """
        INSERT INTO weekly_actuals (week_start, new_actual, pause_actual, source, closed_at)
        SELECT
          p.week_start,
          COALESCE(SUM(CASE WHEN l.start_date BETWEEN p.week_start AND p.week_end THEN 1 ELSE 0 END), 0) AS new_actual,
          COALESCE(SUM(CASE WHEN l.end_date   BETWEEN p.week_start AND p.week_end THEN 1 ELSE 0 END), 0) AS pause_actual,
          'lvt_log' AS source,
          NOW()
        FROM (SELECT %(week_start)s::date AS week_start, %(week_end)s::date AS week_end) p
        LEFT JOIN kpis.lvt_log l ON TRUE
        GROUP BY p.week_start
        ON CONFLICT (week_start) DO UPDATE
          SET new_actual = EXCLUDED.new_actual,
              pause_actual = EXCLUDED.pause_actual,
              source = EXCLUDED.source,
              closed_at = NOW();
        """
        hook.run(sql, parameters=window)
        return {"status": "ok"}

    @task()
    def retrain_km(window: dict):
        """
        lvt_log 기반 KM 재학습
        - 학습 창: [fit_start, fit_end] 사이 start_date
        - as_of = week_end (일요일)
        - 코호트: fst_months in (1,3,6,12)
        - 저장: km_models(S,q,h), model_versions upsert
        """
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        _ensure_km_tables(hook)

        # ----- 데이터 로드
        eng = hook.get_sqlalchemy_engine()
        fit_start, fit_end = window["fit_start"], window["fit_end"]
        as_of = pd.to_datetime(window["week_end"]).date()
        horizon = int(window["horizon_weeks"])

        df = pd.read_sql(
            """
            SELECT lecture_vt_no, student_user_no, fst_months, start_date, end_date
            FROM kpis.lvt_log
            WHERE start_date BETWEEN %(s)s AND %(e)s
              AND fst_months IN (1,3,6,12)
            """,
            eng,
            params={"s": fit_start, "e": fit_end},
        )
        if df.empty:
            return {"status": "no_data"}

        df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
        df["end_date"]   = pd.to_datetime(df["end_date"]).dt.date

        # ----- KM용 표본 변환
        def weeks_between(a, b) -> int:
            return max(0, (pd.to_datetime(b) - pd.to_datetime(a)).days // 7)

        # event: end_date가 있고 as_of 이전/동일이면 1, 그 외 0(검열)
        event = (df["end_date"].notna()) & (df["end_date"] <= as_of)
        df["event"] = np.where(event, 1, 0).astype(int)
        df["survival_time"] = np.where(
            df["event"] == 1,
            [weeks_between(s, e) for s, e in zip(df["start_date"], df["end_date"])],
            [weeks_between(s, as_of) for s in df["start_date"]],
        )

        df = df[df["survival_time"] >= 0].copy()
        if df.empty:
            return {"status": "no_samples"}

        # ----- 정수시점 Kaplan–Meier (주 단위) 구현
        def km_discrete(times: np.ndarray, events: np.ndarray, horizon_weeks: int):
            """
            times: 생존시간(주) 정수, events: 1=이벤트, 0=검열
            반환: S[0..H], q[0..H], h[0..H]
            """
            times = times.astype(int)
            H = horizon_weeks
            # 주차별 이벤트/검열 카운트
            max_t = max(times.max(), H)
            d = np.zeros(max_t + 1, dtype=int)  # events at t
            c = np.zeros(max_t + 1, dtype=int)  # censored at t
            for t, e in zip(times, events):
                if e == 1:
                    d[t] += 1
                else:
                    c[t] += 1

            N = len(times)
            S = np.zeros(H + 1, dtype=float)
            q = np.zeros(H + 1, dtype=float)
            h = np.zeros(H + 1, dtype=float)

            S[0] = 1.0
            n_at_risk = N  # t=0 직전 위험집단
            # t=0은 정의상 q=0, h=0
            for t in range(1, H + 1):
                if n_at_risk > 0:
                    h[t] = min(1.0, max(0.0, d[t] / n_at_risk))
                    S[t] = S[t-1] * (1.0 - h[t])
                    q[t] = max(0.0, S[t-1] - S[t])  # ΔS
                    # 다음 시점 위험집단 업데이트
                    n_at_risk = n_at_risk - d[t] - c[t]
                    if n_at_risk < 0:
                        n_at_risk = 0
                else:
                    S[t] = S[t-1]
                    q[t] = 0.0
                    h[t] = 0.0
            return S, q, h

        rows = []
        for c_val, g in df.groupby("fst_months"):
            times = g["survival_time"].to_numpy()
            events = g["event"].to_numpy()
            S, q, h = km_discrete(times, events, horizon)

            for k in range(0, horizon + 1):
                rows.append({
                    "fit_window_start": fit_start,
                    "fit_window_end": fit_end,
                    "time_unit": "week",
                    "cohort_months": int(c_val),
                    "time_k": int(k),
                    "s": float(S[k]),
                    "q": float(q[k]),   # ΔS(k)
                    "h": float(h[k]),   # hazard(k)
                })

        df_km_out = pd.DataFrame(rows).sort_values(["cohort_months", "time_k"])

        # ----- 저장 (idempotent: 같은 창/단위 삭제 후 삽입)
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM kpis.km_models
                WHERE fit_window_start=%s AND fit_window_end=%s AND time_unit='week'
                """,
                (fit_start, fit_end)
            )
            conn.commit()

        # to_sql
        df_km_out.to_sql("km_models", hook.get_sqlalchemy_engine(),
                         if_exists="append", index=False)

        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO kpis.model_versions
                  (fit_window_start, fit_window_end, time_unit, horizon_weeks, status, created_by, notes)
                VALUES
                  (%s, %s, 'week', %s, 'active', %s, %s)
                ON CONFLICT (fit_window_start, fit_window_end, time_unit)
                DO UPDATE SET
                  horizon_weeks = EXCLUDED.horizon_weeks,
                  status        = 'active',
                  created_at    = now(),
                  created_by    = EXCLUDED.created_by,
                  notes         = EXCLUDED.notes
                """,
                (fit_start, fit_end, horizon, "airflow:weekly", f"Weekly retrain as_of={window['week_end']}")
            )
            conn.commit()

        return {"status": "ok", "rows": int(len(df_km_out))}

    @task()
    def promote_model(_retrain_res: dict):
        """필요시 추가 로직(예: 구 모델 status='archived') — 여기서는 패스"""
        return {"status": "ok"}

    # ---- 의존성 ----
    w = compute_window()
    n = insert_new(w)
    p = update_pause(w)
    a = aggregate_weekly_actuals(w)
    r = retrain_km(w)

    [n, p] >> a >> r >> promote_model(r)

km_weekly_full()
