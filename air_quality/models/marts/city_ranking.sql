with daily as (
    select * from {{ ref('agg_city_daily') }}
    where parameter = 'pm25'
        and measured_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
),

city_avg as (
    select
        city,
        country_code,
        ROUND(AVG(avg_value), 2)        as avg_pm25_30d,
        ROUND(MAX(avg_value), 2)        as max_pm25_30d,
        ROUND(MIN(avg_value), 2)        as min_pm25_30d,
        MAX(risk_score)                 as max_risk_score,
        ROUND(AVG(risk_score), 1)       as avg_risk_score,
        MAX(risk_label)                 as current_risk_label,
        SUM(hours_exceeding_who)        as total_hours_exceeding_who,
        COUNT(DISTINCT measured_date)   as days_with_data
    from daily
    group by 1, 2
),

final as (
    select
        city,
        country_code,
        avg_pm25_30d,
        max_pm25_30d,
        min_pm25_30d,
        avg_risk_score,
        max_risk_score,
        current_risk_label,
        total_hours_exceeding_who,
        days_with_data,

        -- Sıralama (en kirli = 1)
        RANK() OVER (ORDER BY avg_pm25_30d DESC) as pollution_rank
    from city_avg
)

select * from final