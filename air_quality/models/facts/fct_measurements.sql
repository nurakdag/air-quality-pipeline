with measurements as (
    select * from {{ ref('stg_measurements') }}
),

locations as (
    select * from {{ ref('dim_location') }}
),

pollutants as (
    select * from {{ ref('dim_pollutant') }}
),

final as (
    select
        -- Keys
        m.measurement_id,
        m.sensor_id,
        m.location_id,
        m.parameter,

        -- Timestamps
        m.measured_at,
        m.measured_date,
        m.measured_hour,
        m.ingested_at,

        -- Location
        l.city,
        l.country_code,
        l.location_name,
        l.latitude,
        l.longitude,

        -- Pollutant
        p.display_name         as parameter_display_name,
        p.category             as parameter_category,
        p.who_threshold,
        p.is_pollutant,

        -- Measurement
        m.value,
        m.unit,

        -- WHO eşiğini aşıyor mu?
        case
            when p.who_threshold is not null and m.value > p.who_threshold then true
            else false
        end as exceeds_who_threshold,

        -- Kaç kat aşıyor?
        case
            when p.who_threshold is not null and p.who_threshold > 0
            then ROUND(m.value / p.who_threshold, 2)
            else null
        end as who_exceedance_ratio

    from measurements m
    left join locations l on m.location_id = l.location_id
    left join pollutants p on m.parameter = p.parameter
)

select * from final