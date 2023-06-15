CREATE OR REPLACE MODEL 
  `sandbox_us.linear_reg_model`
OPTIONS
  ( MODEL_TYPE='LINEAR_REG',
    MAX_ITERATIONS=5,
    INPUT_LABEL_COLS=['total_amount'],
    MODEL_REGISTRY = 'VERTEX_AI', 
    VERTEX_AI_MODEL_ID = 'linear_reg_model' ) AS
SELECT
  trip_distance, 
  passenger_count, 
  total_amount
FROM
  `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2020`;
