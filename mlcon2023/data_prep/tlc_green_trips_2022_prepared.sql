CREATE TABLE 
    `mlcon2023_us.tlc_green_trips_2022_prepared`
AS SELECT 
    DATETIME_DIFF(dropoff_datetime, pickup_datetime, MINUTE) AS trip_duration,
    trip_distance,
    pickup_location_id,
    dropoff_location_id,
    CAST(EXTRACT(HOUR FROM pickup_datetime) AS STRING) pickup_hour,
    CAST(EXTRACT(MINUTE FROM pickup_datetime) AS STRING) pickup_minute,
    CAST(EXTRACT(DAY FROM pickup_datetime) AS STRING) pickup_day,
    CAST(EXTRACT(WEEK FROM pickup_datetime) AS STRING) pickup_week
FROM 
    `mlcon2023_us.tlc_green_trips_2022`;