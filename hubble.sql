WITH order AS (
    SELECT
        o.*,
        cp.is_sns, 
        cp.prime_member_type, 
        cp.revenue_share_amount,
        cp.display_ads_amt,
        cp.subscription_revenue_amt,
        m.dama_mfg_vendor_code,
        m.dama_mfg_vendor_name,
        m.brand_name,
        m.brand_code
        -- category 
    FROM andes.booker.d_mp_asin_manufacturer m
        RIGHT JOIN andes.booker.D_UNIFIED_CUST_SHIPMENT_ITEMS o
        ON o.region_id = m.region_id
        AND o.marketplace_id = m.marketplace_id
        AND o.asin = m.asin
        RIGHT JOIN andes.contribution_ddl.O_WBR_CP_NA cp
        ON o.region_id = cp.region_id
        AND o.marketplace_id = cp.marketplace_id
        AND o.customer_shipment_item_id = cp.customer_shipment_item_id
    WHERE o.region_id=1
        AND o.marketplace_id = 7
        AND o.is_retail_order_item = 'Y'
        AND o.order_day
        BETWEEN TO_DATE('20240101','YYYYMMDD')
        AND TO_DATE('20241231','YYYYMMDD')
),
cte1 AS (
  SELECT
    dama_mfg_vendor_code, 
    brand_code,
    brand_name,
    -- category
    asin, 
    customer_id, 
    order_datetime,
    is_sns, 
    prime_member_type, 
    revenue_share_amount,
    display_ads_amt, 
    subscription_revenue_amt,
    dama_mfg_vendor_code, 
    dama_mfg_vendor_name,
    --DENSE_RANK() OVER(PARTITION BY dama_mfg_vendor_code, customer_id order by order_datetime asc) as rn,
    LAG(order_datetime) OVER(PARTITION BY dama_mfg_vendor_code, customer_id order by order_datetime) as last_purchase_date,
    LAG(asin) OVER(PARTITION BY dama_mfg_vendor_code, customer_id order by order_datetime) as last_purchase_asin
  FROM orders
  WHERE dama_mfg_vendor_code != 'NaN'
    AND order_item_level_condition != 6
),

cte2 AS (
  SELECT
    asin, 
    dama_mfg_vendor_code, 
    dama_mfg_vendor_name, 
    brand_code, 
    brand_name, 
    customer_id, 
    order_datetime,
    is_sns, 
    prime_member_type,
    -- category
    revenue_share_amount,
    display_ads_amt, 
    subscription_revenue_amt,    
    last_purchase_asin,
    last_purchase_date,
    (
      CASE
      WHEN last_purchase_date IS NULL
      THEN 'new to brand' 
      WHEN extract(DAY FROM order_datetime - last_purchase_date)<= 30
      THEN '1 mo'
      WHEN extract(DAY FROM order_datetime - last_purchase_date)> 30
        and extract(DAY FROM order_datetime - last_purchase_date)<= 60
      THEN '2 mo'
      WHEN extract(DAY FROM order_datetime - last_purchase_date)> 60
        and extract(DAY FROM order_datetime - last_purchase_date) <= 90
      THEN '3 mo'
      WHEN extract(DAY FROM order_datetime - last_purchase_date) > 90 
      THEN '> 3 mo'
      ELSE '/'
      END
    ) AS last_purchase_n_days_ago
  FROM cte1
)

SELECT
    asin, dama_mfg_vendor_code, dama_mfg_vendor_name, brand_name, brand_code, customer_id, order_datetime,
    -- category
    is_sns, prime_member_type,
    last_purchase_asin, last_purchase_date,last_purchase_n_days_ago,
    COUNT(DISTINCT customer_id) AS unique_customer_ct,
    sum(revenue_share_amount),
    sum(display_ads_amt), 
    sum(subscription_revenue_amt),   
FROM cte2
WHERE last_purchase_date IS NULL
  OR extract(day from order_datetime) - (day from last_purchase_date) > 1
GROUP BY dama_mfg_vendor_code, asin,  last_purchase_n_days_ago, order_datetime, last_purchase_asin, last_purchase_date, is_sns, prime_member_type, brand_code, brand_name
    -- category
order by dama_mfg_vendor_code, asin,  last_purchase_n_days_ago ASC