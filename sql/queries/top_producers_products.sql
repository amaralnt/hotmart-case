-- Top 50 producers by revenue (2021)
SELECT 
    p.producer_id,
    SUM(pi.purchase_value) AS total_revenue
FROM purchase p
INNER JOIN product_item pi 
    ON p.prod_item_id = pi.prod_item_id
    AND p.prod_item_partition = pi.prod_item_partition
WHERE 1=1
		AND p.release_date IS NOT NULL
    AND p.order_date BETWEEN '2021-01-01' AND '2021-12-31'
GROUP BY p.producer_id
ORDER BY total_revenue DESC
LIMIT 50;


-- Top 2 products by producer
WITH product_revenue AS (
    SELECT 
        p.producer_id,
        pi.product_id,
        SUM(pi.purchase_value) AS total_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY p.producer_id 
            ORDER BY SUM(pi.purchase_value) DESC
        ) AS product_rank
    FROM purchase p
    INNER JOIN product_item pi 
        ON p.prod_item_id = pi.prod_item_id
        AND p.prod_item_partition = pi.prod_item_partition
    WHERE 1=1
		    AND p.release_date IS NOT NULL
    GROUP BY p.producer_id, pi.product_id
)
SELECT 
    producer_id,
    product_id,
    total_revenue,
    product_rank
FROM product_revenue
WHERE 1=1
		AND product_rank <= 2
ORDER BY producer_id, product_rank;