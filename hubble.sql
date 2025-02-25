DROP TABLE IF EXISTS
    orders
;

CREATE TEMP TABLE
    orders AS (
        SELECT
            asin,
            customer_id,
            order_day,
            region_id,
            marketplace_id
      FROM  andes.booker.D_UNIFIED_CUSTOMER_ORDER_ITEMS o
      WHERE o.region_id=1
          and o.marketplace_id = 7
          and o.is_retail_order_item = 'Y'
          and o.order_datetime BETWEEN TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD')-180 AND TO_DATE('{RUN_DATE_YYYYMMDD}','YYYYMMDD')
          and o.order_item_level_condition != 6
)
;


DROP TABLE IF EXISTS
    orders_manu
;


CREATE TEMP TABLE
    orders_manu AS (
        SELECT
            o.asin,
            o.customer_id,
            o.order_day,
            m.dama_mfg_vendor_code,
            m.dama_mfg_vendor_name,
            m.brand_name,
            m.brand_code
        FROM orders o
          left join andes.booker.d_mp_asin_manufacturer m
            ON o.region_id = m.region_id
            AND o.marketplace_id = m.marketplace_id
            AND o.asin = m.asin
    )
;


DROP TABLE IF EXISTS
    order_metrics
;


CREATE TEMP TABLE
    order_metrics AS (
        SELECT
            asin,
            customer_id,
            order_day,
            dama_mfg_vendor_code,
            dama_mfg_vendor_name,
            brand_name,
            brand_code,
            cp.is_sns,
            cp.prime_member_type,
            cp.revenue_share_amt,
            cp.display_ads_amt,
            cp.subscription_revenue_amt,
        FROM orders_manu o
          left join andes.contribution_ddl.O_WBR_CP_NA cp 
            ON o.marketplace_id = cp.marketplace_id
            AND o.customer_shipment_item_id = cp.customer_shipment_item_id
            AND o.asin = cp.asin
    )
;


DROP TABLE IF EXISTS
    cte1
;

CREATE TEMP TABLE
    cte1 AS (
        SELECT
            dama_mfg_vendor_code,
            dama_mfg_vendor_name,
            brand_code,
            brand_name,
            -- category
            ASIN,
            customer_id,
            order_day,
            is_sns,
            prime_member_type,
            revenue_share_amt,
            display_ads_amt,
            subscription_revenue_amt,
            --DENSE_RANK() OVER(PARTITION BY dama_mfg_vendor_code, customer_id order by order_datetime asc) as rn,
            LAG(order_datetime) OVER (
                PARTITION BY
                    dama_mfg_vendor_code,
                    customer_id
                ORDER BY
                    order_datetime
            ) AS last_purchase_date,
            LAG(ASIN) OVER (
                PARTITION BY
                    dama_mfg_vendor_code,
                    customer_id
                ORDER BY
                    order_datetime
            ) AS last_purchase_asin
        FROM
            order_metrics
        WHERE dama_mfg_vendor_code != 'NaN'
    )
;

DROP TABLE IF EXISTS
    cte2
;

CREATE TEMP TABLE
    cte2 AS (
        SELECT
            ASIN,
            dama_mfg_vendor_code,
            dama_mfg_vendor_name,
            brand_code,
            brand_name,
            customer_id,
            order_datetime,
            is_sns,
            prime_member_type,
            -- category
            revenue_share_amt,
            display_ads_amt,
            subscription_revenue_amt,
            last_purchase_asin,
            last_purchase_date,
            (
                CASE
                    WHEN last_purchase_date IS NULL THEN 'new to brand'
                    WHEN last_purchase_asin BETWEEN order_day - 30 AND 0 THEN '1 mo'
                    WHEN last_purchase_asin BETWEEN order_day - 60 AND 30 THEN '2 mo'
                    WHEN last_purchase_asin BETWEEN order_day - 90 AND 60 THEN '3 mo'
                    WHEN order_datetime - last_purchase_date > 90 THEN '> 3 mo'
                    ELSE '/'
                END
            ) AS last_purchase_n_days_ago
        FROM
            cte1
    )
;

DROP TABLE IF EXISTS
    CAISM.new_to_brand_job_test
;

CREATE TABLE
    CAISM.new_to_brand_job_test AS (
        SELECT
            ASIN,
            dama_mfg_vendor_code,
            dama_mfg_vendor_name,
            brand_name,
            brand_code,
            customer_id,
            order_datetime,
            -- category
            is_sns,
            prime_member_type,
            last_purchase_asin,
            last_purchase_date,
            last_purchase_n_days_ago,
            COUNT(DISTINCT customer_id) AS unique_customer_ct,
            SUM(revenue_share_amt),
            SUM(display_ads_amt),
            SUM(subscription_revenue_amt),
        FROM
            cte2
        WHERE
            last_purchase_date IS NULL
            OR EXTRACT( day  FROM order_datetime ) - ( day FROM last_purchase_date  ) > 1
        GROUP BY
            dama_mfg_vendor_code,
            ASIN,
            last_purchase_n_days_ago,
            order_datetime,
            last_purchase_asin,
            last_purchase_date,
            is_sns,
            prime_member_type,
            brand_code,
            brand_name
            -- category
        ORDER BY
            dama_mfg_vendor_code,
            ASIN,
            last_purchase_n_days_ago ASC
    )