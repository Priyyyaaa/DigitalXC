{{ config(
    version = 1,
    materialized = 'table'  
) }}

with mainsource as (
    select *,
        row_number() over (
            partition by Ticket_ID
            order by Created_Date desc
        ) as row_num
    from {{ source('public', 'servicenow') }}
    where Ticket_ID is not null
),

datacleaned as (
    select
        upper(trim(Ticket_ID)) as Ticket_ID,
        nullif(trim(Category), '') as Category,
        nullif(trim(Sub_Category), '') as Sub_Category,
        nullif(trim(Priority), '') as Priority,
        CAST(NULLIF(trim(CAST(Created_Date AS text)), '') AS timestamp) AS Created_Date,
        CAST(NULLIF(trim(CAST(Resolved_Date AS text)), '') AS timestamp) AS Resolved_Date,
        nullif(trim(Status), '') as Status,
        nullif(trim(Assigned_Group), '') as Assigned_Group,
        nullif(trim(Technician), '') as Technician,
     round(
    (EXTRACT(EPOCH FROM (
        CAST(NULLIF(trim(CAST(Resolved_Date AS text)), '') AS timestamp) -
        CAST(NULLIF(trim(CAST(Created_Date AS text)), '') AS timestamp)
    )) / 3600.0)::numeric,
    0
)
 as Resolution_Time_Hrs,

        nullif(trim(Customer_Impact), '') as Customer_Impact
    from mainsource
)

select
    Ticket_ID,
    Category,
    Sub_Category,
    Priority,
    Created_Date,
    Resolved_Date,
    Status,
    Assigned_Group,
    Technician,
    Resolution_Time_Hrs,
    Customer_Impact,
    -- Extract date components
    extract(year from Created_Date) as Created_Year,
    extract(month from Created_Date) as Created_Month,
    extract(day from Created_Date) as Created_Day
from datacleaned
where Ticket_ID is not null