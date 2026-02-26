with daily as (
    select * from {{ ref('agg_city_daily') }}
    where parameter = 'pm25'
),

final as (
    select
        measured_date,
        city,
        country_code,
        parameter,
        avg_value,
        risk_score,
        risk_label,

        -- 7 günlük hareketli ortalama
        ROUND(AVG(avg_value) OVER (
            PARTITION BY city, parameter
            ORDER BY measured_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) as moving_avg_7d,

        -- 3 günlük hareketli ortalama
        ROUND(AVG(avg_value) OVER (
            PARTITION BY city, parameter
            ORDER BY measured_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) as moving_avg_3d,

        -- Önceki güne göre değişim
        ROUND(avg_value - LAG(avg_value) OVER (
            PARTITION BY city, parameter
            ORDER BY measured_date
        ), 2) as day_over_day_change,

        -- Trend: kötüleşiyor mu?
        case
            when avg_value > LAG(avg_value) OVER (
                PARTITION BY city, parameter
                ORDER BY measured_date
            ) then 'Worsening'
            when avg_value < LAG(avg_value) OVER (
                PARTITION BY city, parameter
                ORDER BY measured_date
            ) then 'Improving'
            else 'Stable'
        end as trend

    from daily
)

select * from final