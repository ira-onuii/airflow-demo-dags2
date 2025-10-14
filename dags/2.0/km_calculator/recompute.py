# -*- coding: utf-8 -*-
"""
KM 파이프라인 (일일 실행 안전 버전)

스케줄: 매일 00:30 Asia/Seoul
윈도우: '막 끝난 주(월~일)'만 재처리 (멱등)
기능:
  1) new_lecture → lvt_log INSERT (중복 방지, 겹침 종료, grade 포함)
  2) pause_lecture → lvt_log UPDATE (열린 episode만 닫기)
  3) weekly_actuals 집계/업서트
  4) KM 재학습 (동일 윈도우 active 존재 시 스킵)
  5) (선택) episode_no 재부여(ROW_NUMBER 기반)

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

FIT_MONTHS = 12         # 재학습 창: 최근 12개월
HORIZON_WEEKS = 52      # KM horizon(주)

# -----------------------------
# 유틸
# -----------------------------
def _monday(ts: pendulum.DateTime) -> pendulum.DateTime:
    return ts - timedelta(days=ts.weekday())

def _week_bounds_last_full_kst(now_kst):
    """
    '막 끝난 주(월~일)'의 (월요일, 일요일)를 date로 반환.
    """
    ref = (now_kst - timedelta(days=1)).date()  # 어제(=date)
    wk_mon = ref - timedelta(days=ref.weekday())  # 월요일(date)
    wk_sun = wk_mon + timedelta(days=6)           # 일요일(date)
    return wk_mon, wk_sun


# -----------------------------
# DDL/제약 보강
# -----------------------------
def _ensure_base_tables(hook: PostgresHook):
    # 1) 테이블이 없으면 생성 (최소 스키마)
    hook.run("""
    CREATE TABLE IF NOT EXISTS kpis.lvt_log (
      lecture_vt_no    varchar NOT NULL,
      student_user_no  bigint,
      fst_months       integer,
      start_date       date,
      end_date         date,
      tutoring_state   varchar(32)
    );
    """)

    # 2) 컬럼 보강 (ALTER TABLE에는 IF NOT EXISTS를 'ADD COLUMN'에만 사용)
    hook.run("ALTER TABLE kpis.lvt_log ADD COLUMN IF NOT EXISTS episode_no integer;")
    hook.run("ALTER TABLE kpis.lvt_log ADD COLUMN IF NOT EXISTS grade varchar(16);")
    hook.run("ALTER TABLE kpis.lvt_log ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now();")
    hook.run("ALTER TABLE kpis.lvt_log ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();")

    # 3) 멱등 유니크 인덱스 (일일 스케줄 대비 중복 방지)
    hook.run("""
    CREATE UNIQUE INDEX IF NOT EXISTS lvt_log_uniq_lvt_startdate
      ON kpis.lvt_log(lecture_vt_no, start_date);
    """)

    # 4) weekly_actuals (PK 포함)
    hook.run("""
    CREATE TABLE IF NOT EXISTS kpis.weekly_actuals (
      week_start    date NOT NULL,
      cohort_months integer NOT NULL,
      new_actual    integer NOT NULL DEFAULT 0,
      pause_actual  integer NOT NULL DEFAULT 0,
      source        varchar(32),
      closed_at     timestamptz,
      PRIMARY KEY (week_start, cohort_months)
    );
    """)

    # 5) km_models / model_versions 기본 스키마 보장
    hook.run("""
    CREATE TABLE IF NOT EXISTS kpis.km_models (
      fit_window_start date,
      fit_window_end   date,
      time_unit        varchar(16),
      cohort_months    integer,
      time_k           integer,
      s double precision,
      q double precision,
      h double precision
    );
    """)
    hook.run("""
    CREATE TABLE IF NOT EXISTS kpis.model_versions (
      fit_window_start date,
      fit_window_end   date,
      time_unit        varchar(16),
      horizon_weeks    integer,
      status           varchar(16) CHECK (status IN ('active','inactive','archived')),
      created_at       timestamptz DEFAULT now(),
      created_by       varchar(64),
      notes            text,
      PRIMARY KEY (fit_window_start, fit_window_end, time_unit)
    );
    """)

# -----------------------------
# DAG
# -----------------------------
@dag(
    dag_id="km_daily_full",
    schedule="30 00 * * *",  # 매일 00:30 KST
    start_date=datetime(2025, 1, 1, tzinfo=SEOUL),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["KM_calculator", "daily", "lvt_log"],
)
def km_daily_full():

    @task()
    def compute_window(**context):
        logical_end_utc = context.get("data_interval_end") or context.get("execution_date")
        now_kst = pendulum.instance(logical_end_utc).in_timezone(SEOUL) if logical_end_utc else pendulum.now(SEOUL)

        week_start, week_end = _week_bounds_last_full_kst(now_kst)  # 둘 다 date

        # pendulum DateTime으로 승격
        fit_end_dt = pendulum.datetime(week_end.year, week_end.month, week_end.day, tz=SEOUL)
        fit_start_dt = (fit_end_dt - pendulum.duration(months=FIT_MONTHS)).start_of("day")
        fit_start_dt = _monday(fit_start_dt)  # 월요일로 보정 (pendulum.DateTime 사용)

        return {
            "week_start": week_start.isoformat(),   # 'YYYY-MM-DD'
            "week_end":   week_end.isoformat(),     # 'YYYY-MM-DD'
            "fit_start":  fit_start_dt.to_date_string(),  # 'YYYY-MM-DD'
            "fit_end":    week_end.isoformat(),     # 'YYYY-MM-DD'
            "horizon_weeks": HORIZON_WEEKS,
        }


    @task()
    def ensure_ddl():
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        _ensure_base_tables(hook)
        return {"status": "ok"}

    @task()
    def insert_new(window: dict):
        """
        new_lecture → lvt_log (멱등)
        - 지난주 start_date 신규만
        - (lecture_vt_no, start_date) UNIQUE로 중복 방지
        - 같은 key의 과거 '열린' episode만 안전 종료 (start_date-1)
        - episode_no는 최초 삽입 시점의 next_ep로 고정 (중복 삽입 시 DO NOTHING)
        """
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        sql = """
        WITH p AS (
  SELECT %(week_start)s::date AS ws, %(week_end)s::date AS we
    ),
    src AS (
      SELECT
        n.lecture_vt_no              AS lecture_vt_no,   -- ★ 캐스팅 제거 (원형 유지)
        n.student_user_no,
        n.fst_months,
        n.start_date::date           AS start_date,
        COALESCE(n.tutoring_state,'') AS tutoring_state,
        n.grade
      FROM kpis.new_lecture n, p
      WHERE n.start_date::date BETWEEN p.ws AND p.we
    ),

    -- 과거 '열린' episode만 종료 (새 시작일 이전분만)
    closed AS (
      UPDATE kpis.lvt_log l
      SET end_date   = GREATEST(l.start_date, s.start_date - INTERVAL '1 day'),
          updated_at = NOW()
      FROM src s
      WHERE l.lecture_vt_no::text = s.lecture_vt_no::text   -- ★ 타입 맞춤 (bigint/text 혼재 대비)
        AND l.end_date IS NULL
        AND l.start_date < s.start_date
      RETURNING l.lecture_vt_no
    ),

    next_ep AS (
      SELECT
        s.*,
        COALESCE((
          SELECT MAX(l.episode_no)
          FROM kpis.lvt_log l
          WHERE l.lecture_vt_no::text = s.lecture_vt_no::text  -- ★ 타입 맞춤
        ),0) + 1 AS episode_no_new
      FROM src s
    ),

    ins AS (
      INSERT INTO kpis.lvt_log
        (lecture_vt_no, episode_no, student_user_no, fst_months,
        start_date, end_date, tutoring_state, grade, created_at, updated_at)
      SELECT
        n.lecture_vt_no,             -- ★ 원형 그대로 넣기(타깃 타입에 DB가 맞춤)
        n.episode_no_new,
        n.student_user_no,
        n.fst_months,
        n.start_date,
        NULL,
        n.tutoring_state,
        n.grade,
        NOW(), NOW()
      FROM next_ep n
      ON CONFLICT (lecture_vt_no, start_date) DO NOTHING
      RETURNING lecture_vt_no, episode_no
    )
    SELECT
      (SELECT COUNT(*) FROM src)    AS source_rows,
      (SELECT COUNT(*) FROM closed) AS closed_rows,
      (SELECT COUNT(*) FROM ins)    AS inserted_rows;
        """
        r = hook.get_first(sql, parameters=window)
        return {"source_rows": int(r[0] or 0), "closed_rows": int(r[1] or 0), "inserted_new": int(r[2] or 0)}

    @task()
    def update_pause(window: dict):
        """
        pause_lecture → lvt_log (멱등)
        - 지난주 end_date만 반영
        - 열린 episode만 닫음
        """
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        sql = """
        WITH p AS (SELECT %(week_start)s::date AS ws, %(week_end)s::date AS we),
        src AS (
          SELECT lecture_vt_no AS lecture_vt_no,
                 end_date::date      AS end_date,
                 COALESCE(tutoring_state,'') AS tutoring_state
          FROM kpis.pause_lecture, p
          WHERE end_date::date BETWEEN p.ws AND p.we
        )
        UPDATE kpis.lvt_log l
        SET end_date   = GREATEST(s.end_date, l.start_date),
            tutoring_state = COALESCE(s.tutoring_state, l.tutoring_state),
            updated_at = NOW()
        FROM src s
        WHERE s.lecture_vt_no = l.lecture_vt_no
          AND l.end_date IS NULL;
        """
        hook.run(sql, parameters=window)

        cnt = hook.get_first(
            "WITH p AS (SELECT %(week_start)s::date ws, %(week_end)s::date we) "
            "SELECT COUNT(*) FROM kpis.lvt_log, p WHERE end_date BETWEEN p.ws AND p.we;",
            parameters=window
        )[0]
        return {"updated_pause": int(cnt or 0)}

    @task()
    def aggregate_weekly_actuals(window: dict):
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        sql = """
        WITH p AS (SELECT %(week_start)s::date ws, %(week_end)s::date we),
        cohorts AS (SELECT 1 c UNION ALL SELECT 3 UNION ALL SELECT 6 UNION ALL SELECT 12),
        agg AS (
          SELECT p.ws AS week_start, c.c AS cohort_months,
                 COALESCE(SUM(CASE WHEN l.start_date BETWEEN p.ws AND p.we THEN 1 END),0) AS new_actual,
                 COALESCE(SUM(CASE WHEN l.end_date   BETWEEN p.ws AND p.we THEN 1 END),0) AS pause_actual
          FROM p CROSS JOIN cohorts c
          LEFT JOIN kpis.lvt_log l ON l.fst_months = c.c
          GROUP BY p.ws, c.c
        )
        INSERT INTO kpis.weekly_actuals (week_start, cohort_months, new_actual, pause_actual, source, closed_at)
        SELECT week_start, cohort_months, new_actual, pause_actual, 'lvt_log', NOW()
        FROM agg
        ON CONFLICT (week_start, cohort_months) DO UPDATE
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
        - 스킵 가드: 동일 (fit_start, fit_end, 'week')가 이미 active이고
                    해당 구간 km_models가 존재하면 재학습 생략
        """
        hook = PostgresHook(postgres_conn_id=CONN_ID)

        # ---- 스킵 가드
        guard_sql = """
        WITH p AS (
          SELECT %(fit_start)s::date AS fs, %(fit_end)s::date AS fe
        )
        SELECT
          EXISTS (
            SELECT 1 FROM kpis.model_versions mv, p
            WHERE mv.fit_window_start = p.fs
              AND mv.fit_window_end   = p.fe
              AND mv.time_unit = 'week'
              AND mv.status    = 'active'
          ) AS has_active,
          EXISTS (
            SELECT 1 FROM kpis.km_models km, p
            WHERE km.fit_window_start = p.fs
              AND km.fit_window_end   = p.fe
              AND km.time_unit = 'week'
          ) AS has_km
        ;
        """
        has_active, has_km = hook.get_first(guard_sql, parameters=window)
        if bool(has_active) and bool(has_km):
            return {"status": "skipped_active_exists"}

        # ---- 데이터 로드
        eng = hook.get_sqlalchemy_engine()
        fit_start = pd.to_datetime(window["fit_start"])
        fit_end   = pd.to_datetime(window["fit_end"])
        as_of     = pd.Timestamp(window["week_end"]).normalize()
        horizon   = int(window["horizon_weeks"])

        df = pd.read_sql(
            """
            SELECT lecture_vt_no, student_user_no, fst_months, start_date, end_date
            FROM kpis.lvt_log
            WHERE start_date BETWEEN %(s)s AND %(e)s
              AND fst_months IN (1,3,6,12)
            """,
            eng, params={"s": fit_start, "e": fit_end},
        )
        if df.empty:
            return {"status": "no_data"}

        def _as_naive_ts(s: pd.Series) -> pd.Series:
            s = pd.to_datetime(s, errors="coerce")
            tz = getattr(s.dtype, "tz", None)
            if tz is not None:
                s = s.dt.tz_convert(None)
            return s.dt.normalize()

        df["start_date"] = _as_naive_ts(df["start_date"])
        df["end_date"]   = _as_naive_ts(df["end_date"])

        def weeks_between(a, b) -> int:
            return max(0, (pd.to_datetime(b) - pd.to_datetime(a)).days // 7)

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

        def km_discrete(times: np.ndarray, events: np.ndarray, horizon_weeks: int):
            times = times.astype(int)
            H = horizon_weeks
            max_t = max(times.max(), H)
            d = np.zeros(max_t + 1, dtype=int)  # events at t
            c = np.zeros(max_t + 1, dtype=int)  # censored at t
            for t, e in zip(times, events):
                if e == 1: d[t] += 1
                else:     c[t] += 1

            N = len(times)
            S = np.zeros(H + 1, dtype=float)
            q = np.zeros(H + 1, dtype=float)
            h = np.zeros(H + 1, dtype=float)

            S[0] = 1.0
            n_at_risk = N
            for t in range(1, H + 1):
                if n_at_risk > 0:
                    h[t] = min(1.0, max(0.0, d[t] / n_at_risk))
                    S[t] = S[t-1] * (1.0 - h[t])
                    q[t] = max(0.0, S[t-1] - S[t])  # ΔS
                    n_at_risk = n_at_risk - d[t] - c[t]
                    if n_at_risk < 0: n_at_risk = 0
                else:
                    S[t] = S[t-1]; q[t] = 0.0; h[t] = 0.0
            return S, q, h

        rows = []
        for c_val, g in df.groupby("fst_months"):
            times = g["survival_time"].to_numpy()
            events = g["event"].to_numpy()
            S, q, h = km_discrete(times, events, horizon)
            for k in range(0, horizon + 1):
                rows.append({
                    "fit_window_start": fit_start.date(),
                    "fit_window_end":   fit_end.date(),
                    "time_unit":        "week",
                    "cohort_months":    int(c_val),
                    "time_k":           int(k),
                    "s": float(S[k]), "q": float(q[k]), "h": float(h[k]),
                })
        df_km_out = pd.DataFrame(rows).sort_values(["cohort_months", "time_k"])

        # 기존 동일 윈도우 삭제 후 저장
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
              DELETE FROM kpis.km_models
              WHERE fit_window_start=%s AND fit_window_end=%s AND time_unit='week'
            """, (fit_start, fit_end))
            conn.commit()

        df_km_out.to_sql(
            "km_models", eng, schema="kpis", if_exists="append", index=False
        )

        # model_versions upsert: 이번 윈도우를 active로 승격
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
            INSERT INTO kpis.model_versions
              (fit_window_start, fit_window_end, time_unit, horizon_weeks, status, created_by, notes)
            VALUES (%s, %s, 'week', %s, 'active', %s, %s)
            ON CONFLICT (fit_window_start, fit_window_end, time_unit)
            DO UPDATE SET
              horizon_weeks = EXCLUDED.horizon_weeks,
              status        = 'active',
              created_at    = now(),
              created_by    = EXCLUDED.created_by,
              notes         = EXCLUDED.notes
            """, (fit_start, fit_end, horizon, "airflow:daily", f"Daily retrain as_of={window['week_end']}"))
            conn.commit()

        return {"status": "ok", "rows": int(len(df_km_out))}

    @task()
    def renumber_episodes():
        """
        (선택) episode_no 재부여:
          - lecture_vt_no별 start_date 오름차순으로 1,2,3… 부여
          - 기존 값과 다르면만 업데이트 → 멱등
        필요 시 수동 트리거/주 1회 스케줄 권장.
        """
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        sql = """
        WITH ordered AS (
          SELECT
            id,                      -- lvt_log에 PK(id)가 있다고 가정(없으면 ctid 사용 가능)
            lecture_vt_no,
            start_date,
            ROW_NUMBER() OVER (PARTITION BY lecture_vt_no ORDER BY start_date, COALESCE(end_date, DATE '2100-01-01')) AS rn
          FROM kpis.lvt_log
        )
        UPDATE kpis.lvt_log l
        SET episode_no = o.rn,
            updated_at = NOW()
        FROM ordered o
        WHERE l.id = o.id
          AND (l.episode_no IS DISTINCT FROM o.rn);
        """
        # 만약 kpis.lvt_log에 PK id가 없다면, 위 SQL에서 id 대신 ctid를 사용하세요.
        # 단, ctid는 VACUUM 등으로 변할 수 있으므로 가능하면 PK 컬럼을 추가하세요.
        try:
            hook.run(sql)
            return {"status": "ok"}
        except Exception as e:
            return {"status": "skip_or_error", "detail": str(e)}

    # ---- 의존성 ----
    w  = compute_window()
    d  = ensure_ddl()
    n  = insert_new(w)
    p  = update_pause(w)
    a  = aggregate_weekly_actuals(w)
    r  = retrain_km(w)
    # 선택: 주 1회만 스케줄하거나 수동 실행 권장
    # ren = renumber_episodes()

    d >> n >> p >> a >> r
    # ren  # 필요 시 별도 트리거

km_daily_full()





# # -*- coding: utf-8 -*-
# """
# KM 파이프라인(주간):
# - 매주 일요일 23:59 (Asia/Seoul) 기준
# 1) new_lecture → lvt_log (INSERT: 열린 episode 없을 때만)
# 2) pause_lecture → lvt_log (UPDATE: 열린 episode만 end_date 세팅)
# 3) weekly_actuals 집계/업서트 (월~일)
# 4) KM 재학습 (as-of = 일요일, lvt_log 기반 검열 포함)
# 5) model_versions 활성화 롤오버(같은 창/단위 upsert)

# 연결: postgres_conn_2.0
# """

# from datetime import datetime, timedelta
# import pendulum
# from airflow.decorators import dag, task
# from airflow.providers.postgres.hooks.postgres import PostgresHook

# import pandas as pd
# import numpy as np

# # -----------------------------
# # 설정
# # -----------------------------
# SEOUL = pendulum.timezone("Asia/Seoul")
# CONN_ID = "postgres_conn_2.0"

# # 재학습 창: 최근 12개월(필요시 바꾸세요)
# FIT_MONTHS = 12
# # KM horizon (주) — 필요시 바꾸세요
# HORIZON_WEEKS = 52

# # -----------------------------
# # 유틸
# # -----------------------------
# def _week_bounds_last_full_kst(now_kst: pendulum.DateTime):
#     """
#     '막 끝난 주(월~일)'를 반환.
#     실행 시각이 언제든 KST 기준 어제 날짜가 속한 주를 집계 대상으로 삼음.
#     """
#     ref = now_kst - timedelta(days=1)           # ← 핵심: 어제
#     wk_mon = _monday(ref).date()                # 월요일(날짜)
#     wk_sun = (pendulum.instance(wk_mon, tz=SEOUL) + timedelta(days=6))
#     return wk_mon, wk_sun

# def _ensure_weekly_actuals(hook: PostgresHook):
#     # 1) 테이블 생성 (없으면). 스키마 명시!
#     hook.run("""
#     CREATE TABLE IF NOT EXISTS kpis.weekly_actuals (
#       week_start date NOT NULL,
#       cohort_months integer NOT NULL,
#       new_actual integer NOT NULL DEFAULT 0,
#       pause_actual integer NOT NULL DEFAULT 0,
#       source varchar(32),
#       closed_at timestamptz
#     );
#     """)

#     # 2) 기존 테이블에 cohort_months 없거나 NULL이면 보정
#     hook.run("ALTER TABLE kpis.weekly_actuals ADD COLUMN IF NOT EXISTS cohort_months integer;")
#     hook.run("UPDATE kpis.weekly_actuals SET cohort_months = 0 WHERE cohort_months IS NULL;")
#     hook.run("ALTER TABLE kpis.weekly_actuals ALTER COLUMN cohort_months SET NOT NULL;")

#     # 3) (week_start, cohort_months) 유니크 보장
#     #    PK 추가가 실패(기존 PK 존재 등)해도, 유니크 인덱스는 반드시 생성
#     try:
#         hook.run("""
#         ALTER TABLE kpis.weekly_actuals
#         ADD CONSTRAINT weekly_actuals_pkey
#         PRIMARY KEY (week_start, cohort_months);
#         """)
#     except Exception:
#         pass  # 이미 PK가 있으면 무시

#     hook.run("""
#     CREATE UNIQUE INDEX IF NOT EXISTS weekly_actuals_week_cohort_uniq
#     ON kpis.weekly_actuals(week_start, cohort_months);
#     """)



# def _ensure_km_tables(hook: PostgresHook):
#     # km_models (kpis 스키마 강제)
#     hook.run("""
#     CREATE TABLE IF NOT EXISTS kpis.km_models (
#       fit_window_start date,
#       fit_window_end   date,
#       time_unit        varchar(16),
#       cohort_months    integer,
#       time_k           integer
#     );
#     """)
#     # 누락 컬럼 보강
#     hook.run("ALTER TABLE kpis.km_models ADD COLUMN IF NOT EXISTS s double precision;")
#     hook.run("ALTER TABLE kpis.km_models ADD COLUMN IF NOT EXISTS q double precision;")
#     hook.run("ALTER TABLE kpis.km_models ADD COLUMN IF NOT EXISTS h double precision;")

#     # model_versions도 kpis로
#     hook.run("""
#     CREATE TABLE IF NOT EXISTS kpis.model_versions (
#       fit_window_start date,
#       fit_window_end   date,
#       time_unit        varchar(16),
#       horizon_weeks    integer,
#       status           varchar(16),
#       created_at       timestamptz DEFAULT now(),
#       created_by       varchar(64),
#       notes            text,
#       PRIMARY KEY (fit_window_start, fit_window_end, time_unit)
#     );
#     """)


# def _monday(ts: pendulum.DateTime) -> pendulum.DateTime:
#     return ts - timedelta(days=ts.weekday())


# # -----------------------------
# # DAG
# # -----------------------------
# @dag(
#     dag_id="km_weekly_full",
#     schedule="30 00 * * MON",  # 매주 일요일 23:59
#     start_date=datetime(2025, 1, 1, tzinfo=SEOUL),
#     catchup=False,
#     default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
#     tags=["KM_calculator", "weekly", "lvt_log"],
# )
# def km_weekly_full():

#     @task()
#     def compute_window(**context):
#         # 가능하면 Airflow logical time 사용(더 결정론적)
#         # data_interval_end가 들어오면 그 시각의 KST-1일을 기준으로 잡아도 됨
#         logical_end_utc = context.get("data_interval_end") or context.get("execution_date")
#         if logical_end_utc:
#             now_kst = pendulum.instance(logical_end_utc).in_timezone(SEOUL)
#         else:
#             now_kst = pendulum.now(SEOUL)

#         week_start, week_end = _week_bounds_last_full_kst(now_kst)

#         # 재학습 윈도우: 최근 FIT_MONTHS개월 (월요일 정렬)
#         fit_end = pendulum.instance(week_end, tz=SEOUL)
#         fit_start_ts = (fit_end - pendulum.duration(months=FIT_MONTHS)).start_of("day")
#         fit_start_ts = _monday(fit_start_ts)  # 월요일 정렬

#         return {
#             "week_start": week_start.isoformat(),          # 'YYYY-MM-DD'
#             "week_end":   week_end.isoformat(),            # 'YYYY-MM-DD'
#             "fit_start":  fit_start_ts.isoformat(), # 'YYYY-MM-DD'
#             "fit_end":    week_end.isoformat(),            # 'YYYY-MM-DD'
#             "horizon_weeks": HORIZON_WEEKS,
#         }
#     ### 고정 window (백필용) ###
#         # return {
#         #   "week_start": "2025-09-01",
#         #   "week_end":   "2025-09-07",
#         #   "fit_start":  "2024-09-02",
#         #   "fit_end":    "2025-09-07",
#         #   "horizon_weeks": 52,
#         # }

#     @task()
#     def insert_new(window: dict):
#         """
#         new_lecture → lvt_log INSERT (정규화 키 + 스칼라 MAX + 겹침 자동 종료)
#         - 이번 주(start_date ∈ [week_start, week_end]) 신규만
#         - lecture_vt_no 표현 차이(선행 0/공백/하이픈/대소문자)를 정규화하여 일치
#         - 열린 episode는 start_date-1 로 닫고, 다음 episode_no로 INSERT
#         - 멱등 보장: ON CONFLICT (lecture_vt_no, episode_no) DO NOTHING
#         """
#         hook = PostgresHook(postgres_conn_id=CONN_ID)
#         sql = """
#         WITH params AS (
#           SELECT %(week_start)s::date AS week_start, %(week_end)s::date AS week_end
#         ),
#         -- 0) 소스 정규화
#         new_src AS (
#           SELECT
#             REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(n.lecture_vt_no::text)), '\\s|-', '', 'g'), '^0+', '') AS norm_key,
#             n.lecture_vt_no        AS lecture_vt_no_raw,
#             n.student_user_no,
#             n.fst_months,
#             n.start_date::date     AS start_date,
#             n.tutoring_state,
#             n.grade
#           FROM kpis.new_lecture n, params p
#           WHERE n.start_date::date BETWEEN p.week_start AND p.week_end
#         ),
#         -- 1) 겹침 자동 종료
#         closed AS (
#           UPDATE kpis.lvt_log l
#           SET end_date  = LEAST((s.start_date - INTERVAL '1 day')::date, (SELECT week_end FROM params)),
#               updated_at = NOW()
#           FROM new_src s
#           WHERE REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(l.lecture_vt_no::text)), '\\s|-', '', 'g'), '^0+', '') = s.norm_key
#             AND l.end_date IS NULL
#           RETURNING l.lecture_vt_no
#         ),
#         -- 2) 삽입 후보: next_ep = (해당 키의) MAX(episode_no) + 1
#         cand AS (
#           SELECT
#             s.norm_key,
#             s.lecture_vt_no_raw,
#             s.student_user_no,
#             s.fst_months,
#             s.start_date,
#             s.tutoring_state,
#             s.grade,
#             COALESCE((
#               SELECT MAX(l.episode_no)
#               FROM kpis.lvt_log l
#               WHERE REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(l.lecture_vt_no::text)), '\\s|-', '', 'g'), '^0+', '') = s.norm_key
#             ), 0) + 1 AS next_ep
#           FROM new_src s
#         ),
#         -- 3) 삽입(필터 없이 시도) + 멱등
#         ins AS (
#           INSERT INTO kpis.lvt_log (
#             lecture_vt_no, episode_no, student_user_no, fst_months,
#             start_date, end_date, tutoring_state, created_at, updated_at,grade
#           )
#           SELECT
#             c.lecture_vt_no_raw,
#             c.next_ep,
#             c.student_user_no,
#             c.fst_months,
#             c.start_date,
#             NULL,
#             c.tutoring_state,
#             NOW(), NOW()
#             , c.grade
#           FROM cand c
#           ON CONFLICT (lecture_vt_no, episode_no) DO NOTHING
#           RETURNING lecture_vt_no, episode_no
#         )
#         SELECT
#           (SELECT COUNT(*)::int FROM new_src) AS source_rows,
#           (SELECT COUNT(*)::int FROM closed)  AS closed_rows,
#           (SELECT COUNT(*)::int FROM cand)    AS candidate_rows,
#           (SELECT COUNT(*)::int FROM ins)     AS inserted_rows;
#         """
#         source_rows, closed_rows, candidate_rows, inserted_rows = hook.get_first(sql, parameters=window)
#         return {
#             "source_rows": int(source_rows or 0),
#             "closed_rows": int(closed_rows or 0),
#             "candidate_rows": int(candidate_rows or 0),
#             "inserted_new": int(inserted_rows or 0),
#         }





#     @task()
#     def update_pause(window: dict):
#         """
#         pause_lecture → lvt_log UPDATE
#         - 이번 주(end_date ∈ [week_start, week_end]) 중단만
#         - 열린 episode(end_date IS NULL)만 닫음
#         - start_date보다 앞설 수 없도록 GREATEST
#         """
#         hook = PostgresHook(postgres_conn_id=CONN_ID)
#         sql = """
#         WITH params AS (
#           SELECT %(week_start)s::date AS week_start, %(week_end)s::date AS week_end
#         ),
#         pause_src AS (
#           SELECT
#             p.lecture_vt_no,
#             p.end_date::date AS pause_date,
#             p.tutoring_state
#           FROM kpis.pause_lecture p, params par
#           WHERE p.end_date::date BETWEEN par.week_start AND par.week_end
#         )
#         UPDATE kpis.lvt_log l
#         SET
#           end_date       = GREATEST(p.pause_date, l.start_date),
#           tutoring_state = COALESCE(p.tutoring_state, l.tutoring_state),
#           updated_at     = NOW()
#         FROM pause_src p
#         WHERE p.lecture_vt_no = l.lecture_vt_no
#           AND l.end_date IS NULL;
#         """
#         hook.run(sql, parameters=window)
#         # 통계
#         cnt_sql = """
#         WITH p AS (SELECT %(week_start)s::date AS ws, %(week_end)s::date AS we)
#         SELECT COUNT(*) FROM kpis.lvt_log, p WHERE end_date BETWEEN p.ws AND p.we;
#         """
#         cnt = hook.get_first(cnt_sql, parameters=window)[0]
#         return {"updated_pause": int(cnt)}

#     @task()
#     def aggregate_weekly_actuals(window: dict):
#         """
#         lvt_log → weekly_actuals 코호트별 집계/업서트
#         기준: 월~일(Asia/Seoul)
#         주의: weekly_actuals PK = (week_start, cohort_months)
#         """
#         hook = PostgresHook(postgres_conn_id=CONN_ID)
#         _ensure_weekly_actuals(hook)  # 이미 (week_start, cohort_months) PK 보장하는 버전

#         sql = """
#         WITH p AS (
#         SELECT %(week_start)s::date AS week_start, %(week_end)s::date AS week_end
#         ),
#         cohorts AS (
#         SELECT 1 AS cohort_months UNION ALL
#         SELECT 3 UNION ALL
#         SELECT 6 UNION ALL
#         SELECT 12
#         ),
#         agg AS (
#         SELECT
#             p.week_start,
#             c.cohort_months,
#             COALESCE(SUM(CASE WHEN l.start_date BETWEEN p.week_start AND p.week_end THEN 1 ELSE 0 END), 0) AS new_actual,
#             COALESCE(SUM(CASE WHEN l.end_date   BETWEEN p.week_start AND p.week_end THEN 1 ELSE 0 END), 0) AS pause_actual
#         FROM p
#         CROSS JOIN cohorts c
#         LEFT JOIN kpis.lvt_log l
#             ON l.fst_months = c.cohort_months
#         GROUP BY p.week_start, c.cohort_months
#         )
#         INSERT INTO kpis.weekly_actuals (week_start, cohort_months, new_actual, pause_actual, source, closed_at)
#         SELECT
#         a.week_start, a.cohort_months, a.new_actual, a.pause_actual, 'lvt_log', NOW()
#         FROM agg a
#         ON CONFLICT (week_start, cohort_months) DO UPDATE
#         SET new_actual = EXCLUDED.new_actual,
#             pause_actual = EXCLUDED.pause_actual,
#             source      = EXCLUDED.source,
#             closed_at   = NOW();
#         """
#         hook.run(sql, parameters=window)
#         return {"status": "ok"}


#     @task()
#     def retrain_km(window: dict):
#         """
#         lvt_log 기반 KM 재학습
#         - 학습 창: [fit_start, fit_end] 사이 start_date
#         - as_of = week_end (일요일)
#         - 코호트: fst_months in (1,3,6,12)
#         - 저장: km_models(S,q,h), model_versions upsert
#         """
#         hook = PostgresHook(postgres_conn_id=CONN_ID)
#         _ensure_km_tables(hook)

#         # ----- 데이터 로드
#         eng = hook.get_sqlalchemy_engine()
#         fit_start = pd.to_datetime(window["fit_start"])
#         fit_end   = pd.to_datetime(window["fit_end"])
#         as_of = pd.Timestamp(window["week_end"]).normalize()
#         horizon   = int(window["horizon_weeks"])
        

#         df = pd.read_sql(
#             """
#             SELECT lecture_vt_no, student_user_no, fst_months, start_date, end_date
#             FROM kpis.lvt_log
#             WHERE start_date BETWEEN %(s)s AND %(e)s
#               AND fst_months IN (1,3,6,12)
#             """,
#             eng,
#             params={"s": fit_start, "e": fit_end},
#         )
#         if df.empty:
#             return {"status": "no_data"}
        
#         # ---- 날짜 컬럼들을 전부 tz-naive Timestamp로 강제 변환 ----
#         def _as_naive_ts(s: pd.Series) -> pd.Series:
#             s = pd.to_datetime(s, errors="coerce")              # object(date) → datetime64[ns] 또는 datetime64[ns, tz]
#             tz = getattr(s.dtype, "tz", None)
#             if tz is not None:                                   # tz-aware → tz-naive
#                 s = s.dt.tz_convert(None)
#             return s.dt.normalize()       

#         df["start_date"] = _as_naive_ts(df["start_date"])
#         df["end_date"]   = _as_naive_ts(df["end_date"])

#         print(df[["start_date","end_date"]].dtypes)  # 둘 다 datetime64[ns] 여야 합니다.
#         print(type(as_of))                           # <class 'pandas._libs.tslibs.timestamps.Timestamp'>


#         # ----- KM용 표본 변환
#         def weeks_between(a, b) -> int:
#             return max(0, (pd.to_datetime(b) - pd.to_datetime(a)).days // 7)

#         # event: end_date가 있고 as_of 이전/동일이면 1, 그 외 0(검열)
#         event = (df["end_date"].notna()) & (df["end_date"] <= as_of)
#         df["event"] = np.where(event, 1, 0).astype(int)
#         df["survival_time"] = np.where(
#             df["event"] == 1,
#             [weeks_between(s, e) for s, e in zip(df["start_date"], df["end_date"])],
#             [weeks_between(s, as_of) for s in df["start_date"]],
#         )

#         df = df[df["survival_time"] >= 0].copy()
#         if df.empty:
#             return {"status": "no_samples"}

#         # ----- 정수시점 Kaplan–Meier (주 단위) 구현
#         def km_discrete(times: np.ndarray, events: np.ndarray, horizon_weeks: int):
#             """
#             times: 생존시간(주) 정수, events: 1=이벤트, 0=검열
#             반환: S[0..H], q[0..H], h[0..H]
#             """
#             times = times.astype(int)
#             H = horizon_weeks
#             # 주차별 이벤트/검열 카운트
#             max_t = max(times.max(), H)
#             d = np.zeros(max_t + 1, dtype=int)  # events at t
#             c = np.zeros(max_t + 1, dtype=int)  # censored at t
#             for t, e in zip(times, events):
#                 if e == 1:
#                     d[t] += 1
#                 else:
#                     c[t] += 1

#             N = len(times)
#             S = np.zeros(H + 1, dtype=float)
#             q = np.zeros(H + 1, dtype=float)
#             h = np.zeros(H + 1, dtype=float)

#             S[0] = 1.0
#             n_at_risk = N  # t=0 직전 위험집단
#             # t=0은 정의상 q=0, h=0
#             for t in range(1, H + 1):
#                 if n_at_risk > 0:
#                     h[t] = min(1.0, max(0.0, d[t] / n_at_risk))
#                     S[t] = S[t-1] * (1.0 - h[t])
#                     q[t] = max(0.0, S[t-1] - S[t])  # ΔS
#                     # 다음 시점 위험집단 업데이트
#                     n_at_risk = n_at_risk - d[t] - c[t]
#                     if n_at_risk < 0:
#                         n_at_risk = 0
#                 else:
#                     S[t] = S[t-1]
#                     q[t] = 0.0
#                     h[t] = 0.0
#             return S, q, h

#         rows = []
#         for c_val, g in df.groupby("fst_months"):
#             times = g["survival_time"].to_numpy()
#             events = g["event"].to_numpy()
#             S, q, h = km_discrete(times, events, horizon)

#             for k in range(0, horizon + 1):
#                 rows.append({
#                     "fit_window_start": fit_start,
#                     "fit_window_end": fit_end,
#                     "time_unit": "week",
#                     "cohort_months": int(c_val),
#                     "time_k": int(k),
#                     "s": float(S[k]),
#                     "q": float(q[k]),   # ΔS(k)
#                     "h": float(h[k]),   # hazard(k)
#                 })

#         df_km_out = pd.DataFrame(rows).sort_values(["cohort_months", "time_k"])

#         # ----- 저장 (항상 kpis.km_models 사용)
#         with hook.get_conn() as conn, conn.cursor() as cur:
#             cur.execute(
#                 """
#                 DELETE FROM kpis.km_models
#                 WHERE fit_window_start=%s AND fit_window_end=%s AND time_unit='week'
#                 """,
#                 (fit_start, fit_end)
#             )
#             conn.commit()

#         # Pandas → SQL: 스키마 지정 + 컬럼 dtype 정리(날짜는 date로 다운캐스트 권장)
#         df_km_out = df_km_out.copy()
#         df_km_out["fit_window_start"] = pd.to_datetime(df_km_out["fit_window_start"]).dt.date
#         df_km_out["fit_window_end"]   = pd.to_datetime(df_km_out["fit_window_end"]).dt.date

#         df_km_out.to_sql(
#             "km_models",
#             hook.get_sqlalchemy_engine(),
#             schema="kpis",              # ✅ 중요
#             if_exists="append",
#             index=False,
#         )

#         with hook.get_conn() as conn, conn.cursor() as cur:
#             cur.execute(
#                 """
#                 INSERT INTO kpis.model_versions
#                 (fit_window_start, fit_window_end, time_unit, horizon_weeks, status, created_by, notes)
#                 VALUES
#                 (%s, %s, 'week', %s, 'active', %s, %s)
#                 ON CONFLICT (fit_window_start, fit_window_end, time_unit)
#                 DO UPDATE SET
#                 horizon_weeks = EXCLUDED.horizon_weeks,
#                 status        = 'active',
#                 created_at    = now(),
#                 created_by    = EXCLUDED.created_by,
#                 notes         = EXCLUDED.notes
#                 """,
#                 (fit_start, fit_end, horizon, "airflow:weekly", f"Weekly retrain as_of={window['week_end']}")
#             )
#             conn.commit()


#         return {"status": "ok", "rows": int(len(df_km_out))}

#     @task()
#     def promote_model(_retrain_res: dict):
#         """필요시 추가 로직(예: 구 모델 status='archived') — 여기서는 패스"""
#         return {"status": "ok"}

#     # ---- 의존성 ----
#     w = compute_window()
#     n = insert_new(w)
#     p = update_pause(w)
#     a = aggregate_weekly_actuals(w)
#     r = retrain_km(w)

#     p >> n >> a >> r >> promote_model(r)

# km_weekly_full()
