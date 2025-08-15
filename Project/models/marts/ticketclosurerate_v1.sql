{{ config(
    version = 1,
    materialized = 'view'  
) }}

with ticketclosurerate as (
        SELECT
            Assigned_Group,
            COUNT(*) AS total_tickets,
            COUNT(CASE WHEN LOWER(Status) = 'closed' THEN 1 END) AS closed_tickets,
            ROUND(
                COUNT(CASE WHEN LOWER(Status) = 'closed' THEN 1 END)::decimal / COUNT(*), 
                2
            ) AS closure_rate
        FROM {{ ref('servicenow_v1') }}
        GROUP BY 1
        order by 4, 1
)
select * from ticketclosurerate 
