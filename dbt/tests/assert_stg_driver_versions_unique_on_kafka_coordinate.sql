-- Same grain and same reasoning as
-- assert_stg_vehicle_versions_unique_on_kafka_coordinate.sql, read that file first.

SELECT bronze_partition, bronze_offset, count(*) AS row_count
  FROM {{ ref('stg_driver_versions') }}
 GROUP BY bronze_partition, bronze_offset
HAVING count(*) > 1
