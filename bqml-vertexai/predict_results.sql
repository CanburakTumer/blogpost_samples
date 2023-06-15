CREATE OR REPLACE TABLE `sandbox_us.predictions`
AS
SELECT
  *
FROM
  ML.PREDICT(MODEL `sandbox_us.linear_reg_model`,
    (
    SELECT
      trip_distance, 
      passenger_count
    FROM
      `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2021`));
