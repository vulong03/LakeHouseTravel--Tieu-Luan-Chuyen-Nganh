-- PostgreSQL script to build a rich dim_date table
-- Date range: 2000-01-01 to 2025-12-31

-- Optional: chọn database/schema
-- CREATE SCHEMA IF NOT EXISTS tourism_dw;
-- SET search_path TO tourism_dw;

DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
  date_sk          INTEGER     PRIMARY KEY,      -- YYYYMMDD
  full_date        DATE        NOT NULL,         -- 2015-01-01
  year             INTEGER     NOT NULL,         -- 2015
  quarter          SMALLINT    NOT NULL,         -- 1-4
  quarter_name     VARCHAR(10) NOT NULL,         -- 'Q1','Q2',...
  month            SMALLINT    NOT NULL,         -- 1-12
  month_name       VARCHAR(15) NOT NULL,         -- 'January'
  year_month       INTEGER     NOT NULL,         -- YYYYMM (e.g. 201501)
  week_of_year     SMALLINT    NOT NULL,         -- 1-53 (ISO week, Mon-based)
  year_week        INTEGER     NOT NULL,         -- YYYYWW (ISOYEAR+ISOWEEK)
  day_of_month     SMALLINT    NOT NULL,         -- 1-31
  day_of_week      SMALLINT    NOT NULL,         -- 1=Monday ... 7=Sunday (ISO)
  day_name         VARCHAR(10) NOT NULL,         -- 'Monday'
  is_weekend       BOOLEAN     NOT NULL,         -- TRUE=Sat/Sun
  is_month_start   BOOLEAN     NOT NULL,
  is_month_end     BOOLEAN     NOT NULL,
  is_quarter_start BOOLEAN     NOT NULL,
  is_quarter_end   BOOLEAN     NOT NULL,
  is_year_start    BOOLEAN     NOT NULL,
  is_year_end      BOOLEAN     NOT NULL
);

-- Populate dim_date using generate_series
INSERT INTO dim_date (
  date_sk,
  full_date,
  year,
  quarter,
  quarter_name,
  month,
  month_name,
  year_month,
  week_of_year,
  year_week,
  day_of_month,
  day_of_week,
  day_name,
  is_weekend,
  is_month_start,
  is_month_end,
  is_quarter_start,
  is_quarter_end,
  is_year_start,
  is_year_end
)
SELECT
  CAST(TO_CHAR(d, 'YYYYMMDD') AS INTEGER)                                 AS date_sk,
  d                                                                       AS full_date,
  EXTRACT(YEAR FROM d)::INT                                              AS year,
  EXTRACT(QUARTER FROM d)::INT                                           AS quarter,
  'Q' || EXTRACT(QUARTER FROM d)::INT                                    AS quarter_name,
  EXTRACT(MONTH FROM d)::INT                                             AS month,
  TO_CHAR(d, 'FMMonth')                                                  AS month_name,
  CAST(TO_CHAR(d, 'YYYYMM') AS INTEGER)                                  AS year_month,
  CAST(TO_CHAR(d, 'IW') AS INTEGER)                                      AS week_of_year,   -- ISO week (1-53)
  (CAST(TO_CHAR(d, 'IYYY') AS INTEGER) * 100
    + CAST(TO_CHAR(d, 'IW') AS INTEGER))                                 AS year_week,      -- ISO year + ISO week
  EXTRACT(DAY FROM d)::INT                                               AS day_of_month,
  EXTRACT(ISODOW FROM d)::INT                                            AS day_of_week,   -- 1=Mon..7=Sun (ISO)
  TO_CHAR(d, 'FMDay')                                                    AS day_name,
  (EXTRACT(ISODOW FROM d) IN (6,7))                                      AS is_weekend,
  (EXTRACT(DAY FROM d) = 1)                                              AS is_month_start,
  (d = (date_trunc('month', d) + INTERVAL '1 month - 1 day')::date)      AS is_month_end,
  (EXTRACT(DAY FROM d) = 1 AND EXTRACT(MONTH FROM d) IN (1,4,7,10))      AS is_quarter_start,
  (
    d = (date_trunc('month', d) + INTERVAL '1 month - 1 day')::date
    AND EXTRACT(MONTH FROM d) IN (3,6,9,12)
  )                                                                      AS is_quarter_end,
  (EXTRACT(MONTH FROM d) = 1  AND EXTRACT(DAY FROM d) = 1)               AS is_year_start,
  (EXTRACT(MONTH FROM d) = 12 AND EXTRACT(DAY FROM d) = 31)              AS is_year_end
FROM generate_series(
    DATE '2000-01-01',
    DATE '2025-12-31',
    INTERVAL '1 day'
) AS gs(d)
ORDER BY d;

-- Test:
-- SELECT * FROM dim_date LIMIT 10;
-- SELECT * FROM dim_date WHERE is_month_end = TRUE LIMIT 10;
-- SELECT * FROM dim_date WHERE is_quarter_start = TRUE;
