with fct as (
    select * from {{ ref('fct_measurements') }}
    where is_pollutant = true
),

daily_agg as (
    select
        measured_date,
        city,
        country_code,
        parameter,
        parameter_display_name,
        parameter_category,
        who_threshold,

        -- Temel istatistikler
        ROUND(AVG(value), 2)                                    as avg_value,
        ROUND(MIN(value), 2)                                    as min_value,
        ROUND(MAX(value), 2)                                    as max_value,
        ROUND(STDDEV(value), 2)                                 as stddev_value,

        -- WHO aşım
        ROUND(AVG(who_exceedance_ratio), 2)                     as avg_who_ratio,
        COUNTIF(exceeds_who_threshold = true)                   as hours_exceeding_who,
        COUNT(*)                                                as measurement_count,

        -- PM2.5 bazlı sağlık risk skoru (1-6)
        case
            when parameter = 'pm25' then
                case
                    when AVG(value) <= 15  then 1
                    when AVG(value) <= 35  then 2
                    when AVG(value) <= 55  then 3
                    when AVG(value) <= 150 then 4
                    when AVG(value) <= 250 then 5
                    else 6
                end
            else null
        end as risk_score,

        case
            when parameter = 'pm25' then
                case
                    when AVG(value) <= 15  then 'Good'
                    when AVG(value) <= 35  then 'Moderate'
                    when AVG(value) <= 55  then 'Unhealthy for Sensitive Groups'
                    when AVG(value) <= 150 then 'Unhealthy'
                    when AVG(value) <= 250 then 'Very Unhealthy'
                    else 'Hazardous'
                end
            else null
        end as risk_label

    from fct
    where measured_date is not null
    group by 1,2,3,4,5,6,7
)

select * from daily_agg