CREATE OR REPLACE MODEL 
  `mlcon2023_us.predict_retail_order_v1`
OPTIONS (
  MODEL_TYPE='LINEAR_REG',
  INPUT_LABEL_COLS=['bottles_sold'],
  MODEL_REGISTRY='VERTEX_AI',
  VERTEX_AI_MODEL_ID='predict_retail_order'
)
AS SELECT
  week_of_year,
  store_number,
  zip_code,
  vendor_number,
  liquor_category,
  item_number,
  bottle_volume_ml,
  bottles_sold
FROM 
  `mlcon2023_us.iowa_liquor_sales_prepared`;