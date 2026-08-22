WITH dates AS (
    SELECT DISTINCT order_date::DATE AS date_day
    FROM {{ ref('int_orders_deduped') }}
)

SELECT
    date_day,
    YEAR(date_day) AS year,
    QUARTER(date_day) AS quarter,
    MONTH(date_day) AS month,
    MONTHNAME(date_day) AS month_name,
    DAY(date_day) AS day,
    DAYOFWEEK(date_day) AS day_of_week,
    DAYNAME(date_day) AS day_name
FROM dates