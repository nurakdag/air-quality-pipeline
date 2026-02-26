    with source as (
    select * from `air-quality-pipeline-488422.raw_air_quality.air_quality_raw`
),

cleaned as (
    select
        -- Timestamps
        TIMESTAMP(ingested_at)                          as ingested_at,
        TIMESTAMP(measured_at)                          as measured_at,
        DATE(TIMESTAMP(measured_at))                    as measured_date,
        EXTRACT(HOUR FROM TIMESTAMP(measured_at))       as measured_hour,

        -- Location
        city,
        country_code,
        CAST(location_id AS INT64)                      as location_id,
        location_name,
        CAST(latitude AS FLOAT64)                       as latitude,
        CAST(longitude AS FLOAT64)                      as longitude,

        -- Sensor & measurement
        CAST(sensor_id AS INT64)                        as sensor_id,
        LOWER(TRIM(parameter))                          as parameter,
        CAST(value AS FLOAT64)                          as value,
        LOWER(TRIM(unit))                               as unit,

        -- Surrogate key for deduplication
        TO_HEX(MD5(CONCAT(
            CAST(sensor_id AS STRING), '|',
            COALESCE(CAST(measured_at AS STRING), ''),  '|',
            LOWER(TRIM(parameter))
        )))                                             as measurement_id

    from source
    where
        value is not null
        and value >= 0
        and measured_at is not null
        and sensor_id is not null
),

deduplicated as (
    select *
    from cleaned
    qualify ROW_NUMBER() OVER (
        PARTITION BY measurement_id
        ORDER BY ingested_at DESC
    ) = 1
)

select * from deduplicated