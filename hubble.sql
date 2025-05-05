---------- + COMPANy & VENDOR CODE
-- asin (incl product category & manufacturer info) & customers
-- + # of orders (one customer could be making multiple orders/buying multiple asins)
DROP TABLE IF EXISTS orders_tmp;
CREATE TEMP TABLE orders_tmp AS (
    SELECT DISTINCT
        o.marketplace_id,
        o.customer_shipment_item_id,
        o.customer_purchase_id,
    	o.asin, 
        (
            case
            when maa.gl_product_group_desc='gl_drugstore'
            then 'Beauty'
            when maa.gl_product_group_desc='gl_pet_products'
            then 'Pets'
            when maa.gl_product_group_desc='gl_baby_product'
            then 'Baby'
            when maa.gl_product_group_desc='gl_beauty'
            then 'Beauty'
            when maa.gl_product_group_desc='gl_luxury_beauty'
            then 'Lux Beauty'
            when maa.gl_product_group_desc='gl_grocery'
            then 'Grocery'
            when maa.gl_product_group_desc ='gl_personal_care_appliances'
            then 'HPC'
            end 
        ) as gl_product_group_desc,
    	o.customer_id, 
        TO_DATE(o.order_datetime, 'YYYY-MM-DD') as order_date,
        -- m.dama_mfg_vendor_code,
        -- m.dama_mfg_vendor_name,
        maa.brand_name,
        maa.brand_code,
        o.shipped_units,
        -- o.subtotal,
        -- o.our_price_discount_amt as total_discount, -- doesn't exist???
        maa.product_category
        -- MAX(c.description) as product_category
    FROM andes.booker.D_UNIFIED_CUST_SHIPMENT_ITEMS o
        LEFT JOIN andes.booker.D_MP_ASIN_ATTRIBUTES maa
        ON maa.asin = o.asin
        AND maa.marketplace_id=o.marketplace_id
        AND maa.region_id = o.region_id
        -- LEFT JOIN andes.booker.d_mp_asin_cats c
        -- on c.product_category = maa.product_category
        LEFT JOIN andes.booker.d_mp_asin_manufacturer m
        ON o.asin = m.asin
    WHERE o.region_id=1
        and o.marketplace_id = 7
        and o.shipped_units > 1
        and o.is_retail_merchant = 'Y'
  		and o.order_datetime BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '365 days' and TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
        and o.order_condition != 6
        and maa.gl_product_group_desc in (
            'gl_pet_products',
            'gl_drugstore',
            'gl_baby_product',
            'gl_beauty',
            'gl_luxury_beauty',
            'gl_grocery',
            'gl_personal_care_appliances'
        )
        and o.customer_purchase_id not in ('-1', '?')
        -- and m.dama_mfg_vendor_code != 'NaN'
        -- and o.customer_id=756758481
    -- GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
);

DROP TABLE IF EXISTS orders;
CREATE TEMP TABLE orders AS (
    SELECT
        t1.marketplace_id,
        t1.customer_shipment_item_id,
        t1.customer_purchase_id,
    	t1.asin, 
        t1.gl_product_group_desc,
    	t1.customer_id, 
        t1.order_date,
        -- t1.dama_mfg_vendor_code,
        -- t1.dama_mfg_vendor_name,
        t1.brand_name,
        t1.brand_code,
        MAX(t1.shipped_units) as shipped_units,
        -- max(t1.subtotal) as subtotal,
        -- t1.our_price_discount_amt as total_discount,
        MAX(t2.description) as product_category_desc
    FROM orders_tmp t1
        left join andes.booker.d_mp_asin_cats t2
        on t1.product_category = t2.product_category
    GROUP BY 1,2,3,4,5,6,7,8,9
);

-- CP metrics
-- purchase timeline flag
DROP TABLE IF EXISTS order_metrics_tmp;
CREATE TEMP TABLE order_metrics_tmp AS (
    SELECT
        o.marketplace_id,
        o.customer_shipment_item_id,
        o.customer_purchase_id,
        o.asin,
        o.gl_product_group_desc,
        o.customer_id,
        o.order_date,
        o.brand_name,
        o.brand_code,
        o.product_category_desc,
        sum(o.shipped_units) as shipped_units,
        sum(cp.revenue_share_amt) as revenue_share_amt,
        sum(cp.display_ads_amt) as display_ads_amt,
        sum(cp.subscription_revenue_amt) as subscription_revenue_amt
    FROM orders o
        LEFT JOIN andes.contribution_ddl.O_WBR_CP_NA cp
        ON o.customer_shipment_item_id = cp.customer_shipment_item_id 
        AND o.asin = cp.asin
    GROUP BY 
        o.marketplace_id,  /* Added table qualifier */
        o.customer_shipment_item_id,
        o.customer_purchase_id,
        o.asin,
        o.gl_product_group_desc,
        o.customer_id,
        o.order_date,
        o.brand_name,
        o.brand_code,
        o.product_category_desc
);


DROP TABLE IF EXISTS order_metrics;
CREATE TEMP TABLE order_metrics AS (
    select
        o.*,
        TO_DATE(LAG(o.order_date) OVER (PARTITION BY o.brand_code, o.customer_id ORDER BY o.order_date),'YYYY-MM-DD') AS last_purchase_date,
        LAG(o.ASIN) OVER (PARTITION BY o.brand_code, o.customer_id ORDER BY o.order_date) AS last_purchase_asin,
        (CASE WHEN last_purchase_date IS NULL THEN 1 ELSE 0 END) as first_purchase_flag,
        (CASE WHEN TO_DATE(last_purchase_date, 'YYYY-MM-DD') BETWEEN TO_DATE(order_date,'YYYY-MM-DD') - interval '30 days' AND TO_DATE(order_date,'YYYY-MM-DD') - interval '1 day' THEN 1 ELSE 0 END) one_mo_return_flag,
        (CASE WHEN TO_DATE(last_purchase_date, 'YYYY-MM-DD') BETWEEN TO_DATE(order_date,'YYYY-MM-DD') - interval '60 days' AND TO_DATE(order_date,'YYYY-MM-DD') - interval '31 day' THEN 1 ELSE 0 END) two_mo_return_flag,
        (CASE WHEN TO_DATE(last_purchase_date, 'YYYY-MM-DD') BETWEEN TO_DATE(order_date,'YYYY-MM-DD') - interval '90 days' AND TO_DATE(order_date,'YYYY-MM-DD') - interval '61 day' THEN 1 ELSE 0 END) third_mo_return_flag,
        (CASE WHEN TO_DATE(last_purchase_date, 'YYYY-MM-DD') < TO_DATE(order_date,'YYYY-MM-DD') - interval '90 days' THEN 1 ELSE 0 END) as after_three_mo_return_flag
    from order_metrics_tmp o
);
 -------------------------------------promotion / benchmark-------------------------------------------------------
-- do promotion stuff first. everything else is non-promo
-- promotion
-- + asin pricing
DROP TABLE IF EXISTS array_data;
CREATE TEMP TABLE array_data as (
    select asin, split_to_array(purchase_order_ids , ',') as order, coupon_quantity
    from andes.pdm.DIM_PROMOTION_ASIN
    where purchase_order_ids is not null
);

DROP TABLE IF EXISTS asin_promos;
CREATE TEMP TABLE asin_promos as (
    select t.asin, coupon_quantity, orders as promo_purchase_id
    from array_data as t
        left join t.order as orders 
        on True
);

DROP TABLE IF EXISTS asin_promos_cp;
CREATE TEMP TABLE asin_promos_cp as (
    SELECT
        t3.customer_shipment_item_id,
        t1.order_id,  
        t3.asin,
        t3.customer_id,
        t2.coupon_quantity,
        t1.units_shipped,
        t1.product_gms
    FROM andes.pdm.FACT_PROMOTION_CP t1
        LEFT JOIN asin_promos t2
        ON t2.asin=t1.asin
        and t1.order_id=t2.promo_purchase_id
        RIGHT JOIN order_metrics t3
        on t3.asin=t1.asin
        and t3.customer_shipment_item_id = t1.customer_shipment_item_id
        and t3.customer_id=t1.customer_id
        and t1.marketplace_key=t3.marketplace_id
    WHERE t1.marketplace_key=7

);

-- if not promotion then...
DROP TABLE IF EXISTS flag_promo;
CREATE TEMP TABLE flag_promo as (
    SELECT 
        *,
        (
            case 
            when 
                t1.customer_shipment_item_id NOT in (select customer_shipment_item_id from asin_promos_cp)
                and t1.customer_purchase_id NOT in (select order_id from asin_promos_cp)
            then 'Not Promo Purchase'
            else 'Promo Purchase'
            end 
        ) as if_promo_flag
    FROM order_metrics t1
);








-- DROP TABLE IF EXISTS cte3;
-- CREATE TEMP TABLE cte3 AS (
--     select
--         t2.asin,
--         t2.customer_shipment_item_id,
--         t4.promo_purchase_id,
--         t1.purchase_order_discount_amount,
--         (
--             case
--             when t2.order_date BETWEEN t1.asin_promo_start_datetime AND t1.asin_promo_end_datetime
--             then 'Promotion Purchase'
--             else 'Not a Promotion Purchase'
--             end
--         ) as promotion_flag,
--         (
--             case
--             when t2.order_date BETWEEN t1.asin_promo_start_datetime AND t1.asin_promo_end_datetime
--             then t3.promotion_type,
--             else 'Not a Promotion Purchase'
--             end
--         ) as promotion_type,
--         (
--             case
--             when t2.order_date BETWEEN t1.asin_promo_start_datetime AND t1.asin_promo_end_datetime
--             then t3.promotion_name,
--             else 'Not a Promotion Purchase'
--             end
--         ) as promotion_name,
--         -- above all promotion-specific
--         t2.gl_product_group_desc,
--         t2.product_category,
--         -- t2.dama_mfg_vendor_code,
--         -- t2.dama_mfg_vendor_name,
--         t2.brand_code,
--         t2.brand_name,
--         t2.customer_id, 
--         t2.order_date,
--         t2.subtotal,
--         t2.shipped_units,
--         -- t2.total_discount,
--         t2.last_purchase_asin,
--         t2.last_purchase_date,
--         t2.last_purchase_n_days_ago,
--         t1.coupon_quantity,
--         t2.revenue_share_amt,
--         t2.display_ads_amt,
--         t2.subscription_revenue_amt
--     from andes.pdm.DIM_PROMOTION_ASIN t1
--         right join order_metrics t2
--         on t1.asin=t2.asin
--         left join andes.pdm.DIM_PROMOTION t3
--         on t1.promotion_key = t3.promotion_key
--         left join asin_promos t4
--         on t4.asin = t1.asin
--         -- and t4.promo_purchase_id=t2.customer_purchase_id
--         -- and t4.promo_purchase_id=t1.purchase_order_ids
--     where t1.region_id=1
--         and t1.marketplace_key=7
--         and t3.region_id=1
--         and t1.asin_approval_status='Approved'
--         and t1.suppression_reason = '' or t1.suppression_reason = '[]' --test if this works
-- );


-- DROP TABLE IF EXISTS pm_sandbox_aqxiao.new_to_brand_job_test2;
-- CREATE TABLE pm_sandbox_aqxiao.new_to_brand_job_test2 AS (
--     SELECT
--         asin,
--         -- current_discount_percent,
--         creation_discount_percent,
--         purchase_order_discount_amount,
--         promotion_type,
--         promotion_title,
--         promotion_name,
--         asin_promo_start_datetime,
--         asin_promo_end_datetime,
--         promotion_period_coupon_quantity,
--         current_discount_percent,
--         gl_product_group_desc,
--         product_category,
--         dama_mfg_vendor_code,
--         dama_mfg_vendor_name,
--         brand_name,
--         brand_code,
--         -- customer_id,
--         ship_day,
--         last_purchase_asin,
--         last_purchase_date,
--         last_purchase_n_days_ago,
--         sum(
--             CASE
--             WHEN last_purchase_n_days_ago='first purchase'
--             THEN 1
--             ELSE 0
--             END
--         ) as if_first_buy,
--         COUNT(DISTINCT customer_id) AS unique_customer_ct,
--         -- SUM(promotion_period_revenue) AS promotion_period_revenue,
--         SUM(revenue_share_amt) AS rev_share_amt,
--         -- SUM(promotion_period_display_ads_amt) as promotion_period_display_ads_amt,
--         SUM(display_ads_amt) AS display_ads_amt,
--         -- SUM(promotion_period_subscription_revenue) as promotion_period_subscription_revenue,
--         SUM(subscription_revenue_amt) AS sub_rev_amt
--     FROM cte3
--     WHERE last_purchase_n_days_ago != '/'
--     GROUP BY 
--         gl_product_group_desc,
--         product_category,
--         asin,
--         -- current_discount_percent,
--         creation_discount_percent,
--         promotion_type,
--         promotion_title,
--         promotion_name,
--         asin_promo_start_datetime,
--         asin_promo_end_datetime,
--         promotion_period_coupon_quantity,
--         purchase_order_discount_amount,
--         current_discount_percent,
--         dama_mfg_vendor_code,
--         dama_mfg_vendor_name,
--         brand_name,
--         brand_code,
--         -- customer_id,
--         ship_day,
--         last_purchase_asin,
--         last_purchase_date,
--         last_purchase_n_days_ago,
--         if_first_buy
--         -- is_sns,
--         -- prime_member_type,
--         -- category
--     ORDER BY
--         gl_product_group_desc,
--         product_category,
--         dama_mfg_vendor_code,
--         asin,
--         last_purchase_n_days_ago ASC
-- );

GRANT ALL ON TABLE pm_sandbox_aqxiao.new_to_brand_job_test2 TO PUBLIC;