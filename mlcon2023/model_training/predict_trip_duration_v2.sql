CREATE OR REPLACE MODEL 
  `mlcon2023_us.predict_trip_duration_v2`
OPTIONS (
  MODEL_TYPE='LINEAR_REG',
  INPUT_LABEL_COLS=['trip_duration'],
  MODEL_REGISTRY='VERTEX_AI',
  VERTEX_AI_MODEL_ID='predict_trip_duration'
)
AS SELECT
  trip_duration,
  trip_distance,
  pickup_hour,
  pickup_minute,
  pickup_day,
  pickup_week
FROM 
  `mlcon2023_us.tlc_green_trips_2022_prepared`;