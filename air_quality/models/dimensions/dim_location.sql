with locations as (
    select distinct
        location_id,
        location_name,
        city,
        country_code,
        latitude,
        longitude
    from {{ ref('stg_measurements') }}
    where location_id is not null
),

final as (
    select
        location_id,
        location_name,
        city,
        country_code,
        latitude,
        longitude,
        -- Power BI map görselleştirmesi için
        CONCAT(CAST(latitude AS STRING), ', ', CAST(longitude AS STRING)) as coordinates
    from locations
    qualify ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY location_id) = 1
)

select * from final