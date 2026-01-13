-- "What is the daily GMV of January seen at the end of January?"
WITH point_in_time_state AS (
    SELECT 
        purchase_id,
        purchase_value,
        order_date,
        subsidiary,
        is_paid,
        ROW_NUMBER() OVER (
            PARTITION BY purchase_id 
            ORDER BY snapshot_date DESC
        ) as rn
    FROM gmv_daily_snapshot
    WHERE snapshot_date <= '2023-01-31'      
)
SELECT 
    order_date,
    subsidiary,
    SUM(purchase_value) as gmv_diario
FROM point_in_time_state
WHERE 1=1
	AND rn = 1
  AND is_paid = true
  AND order_date >= '2023-01-01'            
  AND order_date < '2023-02-01'
GROUP BY order_date, subsidiary
ORDER BY order_date, subsidiary;


-- "What was the daily GMV of January seen in July?"
WITH point_in_time_state AS (
    SELECT 
        purchase_id,
        purchase_value,
        order_date,
        subsidiary,
        is_paid,
        ROW_NUMBER() OVER (
            PARTITION BY purchase_id 
            ORDER BY snapshot_date DESC
        ) as rn
    FROM gmv_daily_snapshot
    WHERE snapshot_date <= '2023-07-31'      
)
SELECT 
    order_date,
    subsidiary,
    SUM(purchase_value) as gmv_diario
FROM point_in_time_state
WHERE 1=1
	AND rn = 1
  AND is_paid = true
  AND order_date >= '2023-01-01'            
  AND order_date < '2023-02-01'
GROUP BY order_date, subsidiary
ORDER BY order_date, subsidiary;


-- "What is the daily GMV by subsidiary current?"
SELECT 
    order_date,
    subsidiary,
    SUM(purchase_value) as gmv_diario
FROM gmv_daily_snapshot
WHERE 1=1
	AND is_current = true
  AND release_date IS NOT NULL
GROUP BY order_date, subsidiary
ORDER BY order_date, subsidiary;