CREATE OR REPLACE TABLE `vlog_us.predictions`
AS
SELECT
  *
FROM
  ML.PREDICT(MODEL `vlog_us.vlog_model`,
    (
    SELECT
      trip_distance, 
      passenger_count, 
      total_amount
    FROM
      `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2021`));
      