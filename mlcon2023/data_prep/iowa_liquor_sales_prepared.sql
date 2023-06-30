CREATE TABLE 
  `mlcon2023_us.iowa_liquor_sales_prepared`
AS SELECT 
  CAST(EXTRACT(WEEK FROM date) AS STRING) week_of_year,
  CAST(store_number AS STRING) store_number, 
  CAST(CAST(CASE WHEN ENDS_WITH(zip_code, '.0') THEN SUBSTR(zip_code, 1, 5) ELSE zip_code END AS INTEGER) AS STRING) zip_code,
  CAST(CAST(CASE WHEN ENDS_WITH(vendor_number, '.0') THEN SUBSTR(vendor_number, 1, INSTR(vendor_number, '.')-1) ELSE vendor_number END AS INTEGER) AS STRING) vendor_number,
  CAST(CAST(CASE WHEN ENDS_WITH(category, '.0') THEN SUBSTR(category, 1, INSTR(category, '.')-1) ELSE category END AS INTEGER) AS STRING)  liquor_category,
  CAST(item_number AS STRING) item_number,
  bottle_volume_ml,
  bottles_sold  
FROM 
  `mlcon2023_us.iowa_liquor_sales` 
WHERE 
  zip_code != '712-2'
  AND vendor_number IS NOT NULL
  AND category IS NOT NULL
  AND LENGTH(category) != 8;