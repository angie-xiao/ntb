-- asin & customers
-- + # of orders (one customer could be making multiple orders/buying multiple asins)
DROP TABLE IF EXISTS orders;
CREATE TEMP TABLE orders AS (
    SELECT DISTINCT  
    	o.customer_shipment_item_id, 
    	o.asin, 
        maa.gl_product_group_desc,
        maa.product_category, -- use another table?
    	o.customer_id, 
    	o.ship_day -- use order date in future iterations
    FROM andes.booker.D_UNIFIED_CUST_SHIPMENT_ITEMS o
        left join andes.booker.D_MP_ASIN_ATTRIBUTES maa
        on maa.asin = o.asin
        and maa.marketplace_id=o.marketplace_id
        and maa.region_id = o.region_id
    WHERE o.region_id=1
        and o.marketplace_id = 7
        and o.is_retail_merchant = 'Y'
  		-- and o.ship_day BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '120 days' and TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
        and o.order_condition != 6
        and maa.gl_product_group_desc in (
            'gl_pet_products',
            'gl_drugstore',
            'gl_baby_product',
            'gl_beauty',
            'gl_luxury_beauty',
            'gl_grocery'
        )
);

-- manufacturer
DROP TABLE IF EXISTS orders_manu;
CREATE TEMP TABLE orders_manu AS (
    SELECT
        o.customer_shipment_item_id,
    	o.asin,
        o.customer_id,
    	o.ship_day,
        (
            case
            when gl_product_group_desc='gl_drugstore'
            then 'HPC'
            when gl_product_group_desc='gl_pet_products'
            then 'Pets'
            when gl_product_group_desc='gl_baby_product'
            then 'Baby'
            when gl_product_group_desc='gl_beauty'
            then 'Beauty'
            when gl_product_group_desc='gl_luxury_beauty'
            then 'Lux Beauty'
            when gl_product_group_desc='gl_grocery'
            then 'Grocery'
            end 
        ) as gl_product_group_desc,
        o.product_category,
        m.dama_mfg_vendor_code,
        m.dama_mfg_vendor_name,
        m.brand_name,
        m.brand_code
    FROM orders o
        INNER JOIN andes.booker.d_mp_asin_manufacturer m
        ON o.asin = m.asin
  WHERE m.region_id = 1 AND m.marketplace_id=7
);

-- metrics
DROP TABLE IF EXISTS order_metrics;
CREATE TEMP TABLE order_metrics AS (
    SELECT
        o.asin,
        o.gl_product_group_desc,
        o.product_category,
        o.customer_shipment_item_id,
        o.customer_id,
        o.ship_day,
        o.dama_mfg_vendor_code,
        o.dama_mfg_vendor_name,
        o.brand_name,
        o.brand_code,
        -- cp.is_sns,
        -- cp.prime_member_type,
        cp.revenue_share_amt,
        cp.display_ads_amt,
        cp.subscription_revenue_amt
    FROM orders_manu o
        left join andes.contribution_ddl.O_WBR_CP_NA cp     
        ON o.ship_day = cp.ship_day
        AND o.customer_shipment_item_id = cp.customer_shipment_item_id 
        AND o.asin = cp.asin 
    WHERE cp.marketplace_id = 7 
        -- AND TO_DATE(cp.ship_day,'YYYY-MM-DD') BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '120 days' and TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
		AND cp.marketplace_id=7
);

-- lag
DROP TABLE IF EXISTS cte1;
CREATE TEMP TABLE cte1 AS (
    SELECT
        gl_product_group_desc,
        product_category,
        dama_mfg_vendor_code,
        dama_mfg_vendor_name,
        brand_code,
        brand_name,
        asin,
        customer_id,
        ship_day,
        revenue_share_amt,
        display_ads_amt,
        subscription_revenue_amt,
        LAG(ship_day) OVER (PARTITION BY brand_code, customer_id ORDER BY ship_day) AS last_purchase_date,
        LAG(ASIN) OVER (PARTITION BY brand_code, customer_id ORDER BY ship_day) AS last_purchase_asin
    FROM order_metrics
    WHERE dama_mfg_vendor_code != 'NaN'
);

-- last_purchase_n_days_ago
DROP TABLE IF EXISTS cte2;
CREATE TEMP TABLE cte2 AS (
    SELECT
        gl_product_group_desc,
        product_category,
        asin,
        dama_mfg_vendor_code,
        dama_mfg_vendor_name,
        brand_code,
        brand_name,
        customer_id,
        ship_day,
        revenue_share_amt,
        display_ads_amt,
        subscription_revenue_amt,
        last_purchase_asin,
        last_purchase_date,
        ( 
            CASE
                WHEN last_purchase_date IS NULL THEN 'first purchase'
                WHEN TO_DATE(last_purchase_date, 'YYYY-MM-DD') BETWEEN TO_DATE(ship_day,'YYYY-MM-DD') - interval '30 days' AND TO_DATE(ship_day,'YYYY-MM-DD') - interval '1 day' THEN 'return in 1 mo'
                WHEN TO_DATE(last_purchase_date,'YYYY-MM-DD') BETWEEN TO_DATE(ship_day,'YYYY-MM-DD') - interval '60 days' AND TO_DATE(ship_day,'YYYY-MM-DD')- interval '31 days' THEN 'return in 2 mo'
                WHEN TO_DATE(last_purchase_date,'YYYYMMDD') BETWEEN TO_DATE(ship_day,'YYYY-MM-DD')  - interval '90 days' AND TO_DATE(ship_day, 'YYYY-MM-DD') -  interval '61 days' THEN 'return in 3 mo'
                WHEN TO_DATE(ship_day,'YYYY-MM-DD') - TO_DATE(last_purchase_date,'YYYY-MM-DD') > interval '90 days' THEN 'return after 3 mo+'
                ELSE '/'
            END
        ) AS last_purchase_n_days_ago
    FROM  cte1
);

-- promotion
-- + asin pricing
-- + deal name (PD, Pet Day etc.)
-- change ship day to order day
DROP TABLE IF EXISTS cte3;
CREATE TEMP TABLE cte3 AS (
    select
        t1.asin,
        t1.promotion_amount,
        t1.asin_promo_start_datetime,
        t1.asin_promo_end_datetime,
        t1.coupon_quantity as promotion_period_coupon_quantity,
        t1.purchase_order_discount_amount,
        t1.current_discount_percent,
        -- above all promotion-specific
        t2.gl_product_group_desc,
        t2.product_category,
        t2.dama_mfg_vendor_code,
        t2.dama_mfg_vendor_name,
        t2.brand_code,
        t2.brand_name,
        t2.customer_id,
        t2.ship_day,
        t2.revenue_share_amt,
        (
            CASE
            WHEN t2.ship_day BETWEEN t1.asin_promo_start_datetime AND t1.asin_promo_end_datetime
            then t2.revenue_share_amt 
            else 0
            end
        ) as promotion_period_revenue,
        t2.display_ads_amt,
                (
            CASE
            WHEN t2.ship_day BETWEEN t1.asin_promo_start_datetime AND t1.asin_promo_end_datetime
            then t2.display_ads_amt 
            else 0
            end
        ) as promotion_period_display_ads_amt,
        (
            CASE
            WHEN t2.ship_day BETWEEN t1.asin_promo_start_datetime AND t1.asin_promo_end_datetime
            then t2.subscription_revenue_amt 
            else 0
            end
        ) as promotion_period_subscription_revenue,
        t2.subscription_revenue_amt,
        t2.last_purchase_asin,
        t2.last_purchase_date,
        t2.last_purchase_n_days_ago
    from andes.pdm.DIM_PROMOTION_ASIN t1
        left join cte2
        on t1.asin=t2.asin
    where t1.region_id=1
        and t1.marketplace_key=7
        and t1.asin_approval_status='Approved'
        and t1.suppression_reason = '' or t1.suppression_reason = '[]' --test if this works
);

DROP TABLE IF EXISTS pm_sandbox_aqxiao.new_to_brand_job_test2;
CREATE TABLE pm_sandbox_aqxiao.new_to_brand_job_test2 AS (
    SELECT
        asin,
        promotion_amount,
        -- promotion name
        promotion_period_coupon_quantity,
        purchase_order_discount_amount,
        current_discount_percent,
        gl_product_group_desc,
        product_category,
        dama_mfg_vendor_code,
        dama_mfg_vendor_name,
        brand_name,
        brand_code,
        customer_id,
        ship_day,
        last_purchase_asin,
        last_purchase_date,
        last_purchase_n_days_ago,
        (
            CASE
            WHEN last_purchase_n_days_ago='first purchase'
            THEN 'First Purchases'
            ELSE 'Return Purchases'
            END
        ) as if_first_buy,
        COUNT(DISTINCT customer_id) AS unique_customer_ct,
        SUM(promotion_period_revenue) AS promotion_period_revenue,
        SUM(revenue_share_amt) AS rev_share_amt,
        SUM(promotion_period_display_ads_amt) as promotion_period_display_ads_amt,
        SUM(display_ads_amt) AS display_ads_amt,
        SUM(promotion_period_subscription_revenue) as promotion_period_subscription_revenue,
        SUM(subscription_revenue_amt) AS sub_rev_amt
    FROM cte3
    WHERE last_purchase_n_days_ago != '/'
    GROUP BY 
        gl_product_group_desc,
        product_category,
        asin,
        promotion_amount,
        -- promotion name,
        promotion_period_coupon_quantity,
        purchase_order_discount_amount,
        current_discount_percent,
        dama_mfg_vendor_code,
        dama_mfg_vendor_name,
        brand_name,
        brand_code,
        customer_id,
        ship_day,
        last_purchase_asin,
        last_purchase_date,
        last_purchase_n_days_ago,
        if_first_buy
        -- is_sns,
        -- prime_member_type,
        -- category
    ORDER BY
        gl_product_group_desc,
        product_category,
        dama_mfg_vendor_code,
        asin,
        last_purchase_n_days_ago ASC
);

GRANT ALL ON TABLE pm_sandbox_aqxiao.new_to_brand_job_test2 TO PUBLIC;

