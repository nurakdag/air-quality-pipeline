with temp as (
    select
        city,
        measured_date,
        measured_hour,
        AVG(value) as temperature
    from {{ ref('fct_measurements') }}
    where parameter = 'temperature'
    group by 1,2,3
),

pm25 as (
    select
        city,
        measured_date,
        measured_hour,
        AVG(value) as pm25_value
    from {{ ref('fct_measurements') }}
    where parameter = 'pm25'
    group by 1,2,3
),

final as (
    select
        t.city,
        t.measured_date,
        t.measured_hour,
        t.temperature,
        p.pm25_value
    from temp t
    inner join pm25 p
        on t.city = p.city
        and t.measured_date = p.measured_date
        and t.measured_hour = p.measured_hour
)

select * from final