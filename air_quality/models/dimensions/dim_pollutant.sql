with pollutants as (
    select distinct parameter, unit
    from {{ ref('stg_measurements') }}
),

final as (
    select
        parameter,
        unit,
        case parameter
            when 'pm25'        then 'PM2.5'
            when 'pm10'        then 'PM10'
            when 'pm1'         then 'PM1'
            when 'pm4'         then 'PM4'
            when 'no2'         then 'Nitrogen Dioxide'
            when 'no'          then 'Nitric Oxide'
            when 'nox'         then 'Nitrogen Oxides'
            when 'o3'          then 'Ozone'
            when 'so2'         then 'Sulfur Dioxide'
            when 'co'          then 'Carbon Monoxide'
            when 'co2'         then 'Carbon Dioxide'
            when 'bc'          then 'Black Carbon'
            when 'temperature' then 'Temperature'
            when 'humidity'    then 'Humidity'
            else parameter
        end as display_name,
        case parameter
            when 'pm25' then 'Particulate Matter'
            when 'pm10' then 'Particulate Matter'
            when 'pm1'  then 'Particulate Matter'
            when 'pm4'  then 'Particulate Matter'
            when 'no2'  then 'Nitrogen Compounds'
            when 'no'   then 'Nitrogen Compounds'
            when 'nox'  then 'Nitrogen Compounds'
            when 'o3'   then 'Ozone'
            when 'so2'  then 'Sulfur Compounds'
            when 'co'   then 'Carbon Compounds'
            when 'co2'  then 'Carbon Compounds'
            when 'bc'   then 'Black Carbon'
            when 'temperature' then 'Meteorological'
            when 'humidity'    then 'Meteorological'
            else 'Other'
        end as category,
        -- WHO eşik değerleri (µg/m³, yıllık ortalama)
        case parameter
            when 'pm25' then 15.0
            when 'pm10' then 45.0
            when 'no2'  then 25.0
            when 'o3'   then 60.0
            when 'so2'  then 40.0
            else null
        end as who_threshold,
        case parameter
            when 'pm25'        then true
            when 'pm10'        then true
            when 'no2'         then true
            when 'o3'          then true
            when 'so2'         then true
            when 'co'          then true
            when 'bc'          then true
            when 'temperature' then false
            when 'humidity'    then false
            else true
        end as is_pollutant
    from pollutants
)

select * from final