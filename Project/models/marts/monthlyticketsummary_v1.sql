{{ config(
    version = 1,
    materialized = 'view'  
) }}


with monthlyticketsummary as (
    SELECT
        TO_CHAR(Created_Date, 'Mon-YYYY') AS MonthYear,
        COUNT(*) AS total_tickets,
        ROUND(AVG(Resolution_Time_Hrs), 2) AS avg_resolution_time,
        ROUND(
            COUNT(CASE WHEN LOWER(Status) = 'closed' THEN 1 END)::decimal / COUNT(*),
            2
        ) AS closure_rate
    FROM {{ ref('servicenow_v1') }}
    GROUP BY 1
    ORDER BY 1
)
select * from monthlyticketsummary