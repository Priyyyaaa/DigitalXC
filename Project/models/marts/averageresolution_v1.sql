{{ config(
    version = 1,
    materialized = 'view'  
) }}

with avg_resolution as (
    select Category,
     Priority,
    AVG(Resolution_Time_Hrs) AS avg_resolution
    FROM {{ ref('servicenow_v1') }}
    WHERE Status = 'Resolved' and Resolution_Time_Hrs is not null
    group by Category, Priority
)
select * from avg_resolution