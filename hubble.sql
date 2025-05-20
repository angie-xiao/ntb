---------- + COMPANy & VENDOR CODE
-- asin (incl product category & manufacturer info) & customers
-- + # of orders (one customer could be making multiple orders/buying multiple asins)
-- First table: Combine orders and attributes

DROP TABLE IF EXISTS orders;
CREATE TEMP TABLE orders AS (
    SELECT DISTINCT
        o.marketplace_id,
        o.customer_shipment_item_id,
        o.customer_purchase_id,
        o.asin, 
        CASE
            when maa.gl_product_group_desc = 'gl_drugstore' then 'Beauty'
            when maa.gl_product_group_desc = 'gl_pet_products' then 'Pets'
            when maa.gl_product_group_desc = 'gl_baby_product' then 'Baby'
            when maa.gl_product_group_desc = 'gl_beauty' then 'Beauty'
            when maa.gl_product_group_desc = 'gl_luxury_beauty' then 'Lux Beauty'
            when maa.gl_product_group_desc = 'gl_grocery' then 'Grocery'
            when maa.gl_product_group_desc = 'gl_personal_care_appliances' or maa.gl_product_group_desc = 'gl_drugstore' then 'HPC'
        end as gl_product_group_desc,
        o.customer_id,
        TO_DATE(o.order_datetime, 'YYYY-MM-DD') as order_date,
        maa.brand_name,
        maa.brand_code,
        o.shipped_units,
        c.description as product_category_desc
    FROM andes.booker.D_UNIFIED_CUST_SHIPMENT_ITEMS o
        LEFT JOIN andes.booker.D_MP_ASIN_ATTRIBUTES maa
            ON maa.asin = o.asin
            AND maa.marketplace_id = o.marketplace_id
            AND maa.region_id = o.region_id
        LEFT JOIN andes.booker.d_mp_asin_cats c
            ON maa.product_category = c.product_category
    WHERE o.region_id = 1
        AND o.marketplace_id = 7
        AND maa.gl_product_group IN (
            510, --lux
            364, --personal care appliance
            325, --grocery
            199, --pets
            194, --beauty
            121, --hpc
            75 --baby
        )
        AND o.shipped_units > 1
        AND o.is_retail_merchant = 'Y'
        AND o.order_datetime BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '386 day'  -- 365 days + 3 weeks
            AND TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
        AND o.order_condition != 6
        AND o.customer_purchase_id NOT IN ('-1', '?')
);



-- Order metrics with purchase timeline flags
DROP TABLE IF EXISTS order_metrics;
CREATE TEMP TABLE order_metrics AS (
    WITH order_cp AS (
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
        SUM(o.shipped_units) as shipped_units,
        MAX(o.product_category_desc) as product_category_desc,
        SUM(cp.revenue_share_amt) as revenue_share_amt,
        SUM(cp.display_ads_amt) as display_ads_amt,
        SUM(cp.subscription_revenue_amt) as subscription_revenue_amt
        FROM orders o
        LEFT JOIN andes.contribution_ddl.O_WBR_CP_NA cp
            ON o.customer_shipment_item_id = cp.customer_shipment_item_id 
            AND o.asin = cp.asin
        GROUP BY 
            o.marketplace_id, o.customer_shipment_item_id, o.customer_purchase_id,
            o.asin, o.gl_product_group_desc, o.customer_id, o.order_date,
            o.brand_name, o.brand_code 
    ),
    last_purchase AS (
        SELECT 
            oc.*,
            LAG(order_date) OVER (
                PARTITION BY customer_id, brand_code 
                ORDER BY order_date
            ) as previous_order_date
        FROM order_cp oc
    )
    SELECT
        oc.*,
        CASE 
            WHEN previous_order_date IS NULL THEN 1 
            ELSE 0 
        END as first_purchase_flag,
        CASE 
            WHEN previous_order_date BETWEEN order_date - interval '30 day' 
                AND order_date - interval '1 day' THEN 1
            ELSE 0 
        END as one_mo_return_flag,
        CASE 
            WHEN previous_order_date BETWEEN order_date - interval '60 day' 
                AND order_date - interval '31 day' THEN 1
            ELSE 0 
        END as two_mo_return_flag,
        CASE 
            WHEN previous_order_date BETWEEN order_date - interval '90 day' 
                AND order_date - interval '61 day' THEN 1
            ELSE 0 
        END as third_mo_return_flag,
        CASE 
            WHEN previous_order_date < order_date - interval '90 day' THEN 1
            ELSE 0 
        END as three_mo_plus_return_flag
    FROM last_purchase oc
);


-- Per ASIN per day, including promo info, whenever applies
DROP TABLE IF EXISTS asin_promos_cp;    
CREATE TEMP TABLE asin_promos_cp AS (
    
    WITH promotion_data AS (
        SELECT DISTINCT
            t2.asin,
            t1.customer_shipment_item_id,
            t1.promotion_key,
            t4.promotion_type,
            TO_DATE(t4.start_datetime, 'YYYY-MM-DD') as promo_start_date,
            TO_DATE(t4.end_datetime, 'YYYY-MM-DD') as promo_end_date,
            t4.promotion_internal_title
        FROM andes.pdm.DIM_PROMOTION_ASIN t2
        INNER JOIN andes.pdm.FACT_PROMOTION_CP t1
            ON t2.promotion_key = t1.promotion_key
            AND t1.region_id = 1
            AND t1.marketplace_key = 7
        INNER JOIN andes.pdm.DIM_PROMOTION t4
            ON t4.marketplace_key = t1.marketplace_key
            AND t4.promotion_key = t1.promotion_key
            AND t4.start_datetime BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '386 day'
                AND TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
        WHERE t2.product_group_key in (510, 364, 325, 199, 194, 121, 75)
    )

    SELECT
        t3.marketplace_id,
        t3.asin, 
        t3.gl_product_group_desc,
        t3.order_date,
        t3.brand_name,
        t3.brand_code,
        t3.product_category_desc,
        p.promo_start_date,
        p.promo_end_date,
        COALESCE(p.promotion_type, 'NO_PROMO') as promotion_type,
        DATE_PART('year', t3.order_date) as promo_yr,
        (CASE 
            WHEN p.start_datetime IS NULL OR p.end_datetime IS NULL THEN 0
            ELSE GREATEST(DATEDIFF('day', p.start_datetime, p.end_datetime), 0) + 1
        END) as promo_duration_days,        -- Handle duration calculation
        (CASE 
            WHEN p.promotion_key IS NULL THEN 'N' 
            ELSE 'Y' 
        END) as is_deal_purchase,
        (CASE 
            WHEN p.promotion_key IS NULL THEN 'NO_PROMOTION'
            WHEN UPPER(p.promotion_internal_title) LIKE '%DAILY%ESSENTIAL%' 
                OR UPPER(p.promotion_internal_title) LIKE '%DE%' THEN 'DAILY ESSENTIALS'
            WHEN UPPER(p.promotion_internal_title) LIKE '%BSS%' THEN 'BSS'
            WHEN UPPER(p.promotion_internal_title) LIKE '%PET%DAY%' 
                OR UPPER(p.promotion_internal_title) LIKE '%PET%MONTH%' THEN 'PET DAY'
            WHEN UPPER(p.promotion_internal_title) LIKE '%PBDD%' THEN 'PBDD'
            WHEN UPPER(p.promotion_internal_title) LIKE '%PRIME%DAY%'
                OR UPPER(p.promotion_internal_title) LIKE '%PD%' THEN 'PRIME DAY'
            WHEN UPPER(p.promotion_internal_title) LIKE '%T5%'
                OR UPPER(p.promotion_internal_title) LIKE '%T11%'
                OR UPPER(p.promotion_internal_title) LIKE '%T12%' THEN 'T5/11/12'
            WHEN UPPER(p.promotion_internal_title) LIKE '%BF%'
                OR UPPER(p.promotion_internal_title) LIKE '%BLACK%FRIDAY%' THEN 'BLACK FRIDAY'
            WHEN UPPER(p.promotion_internal_title) LIKE '%CYBER%MONDAY%'
                OR UPPER(p.promotion_internal_title) LIKE '%CM%' THEN 'CYBER MONDAY'
            WHEN UPPER(p.promotion_internal_title) LIKE '%HOLIDAY%' THEN 'HOLIDAY'
            WHEN UPPER(p.promotion_internal_title) LIKE '%BOXING WEEK%'
                OR UPPER(p.promotion_internal_title) LIKE '%BOXING DAY%' THEN 'BOXING WEEK'
            WHEN p.promotion_type = 'Sales Discount' THEN 'SALES DISCOUNT'
            WHEN p.promotion_type = 'Coupon' THEN 'COUPON'
            ELSE 'OTHER'
        END) as event_name,
        SUM(t3.shipped_units) as shipped_units,
        SUM(t3.revenue_share_amt) as revenue_share_amt,
        SUM(t3.display_ads_amt) as display_ads_amt,
        SUM(t3.subscription_revenue_amt) as subscription_revenue_amt,
        SUM(t3.three_mo_plus_return_flag) as three_mo_plus_return_customers,
        SUM(t3.third_mo_return_flag) as three_mo_return_customers,
        SUM(t3.two_mo_return_flag) as two_mo_return_customers,
        SUM(t3.one_mo_return_flag) as one_mo_return_customers,
        SUM(t3.first_purchase_flag) as first_purchase_customers
    FROM order_metrics t3
        LEFT JOIN promotion_data p
            ON t3.asin = p.asin
            AND t3.customer_shipment_item_id = p.customer_shipment_item_id
    GROUP BY 
        t3.marketplace_id,
        t3.asin, 
        t3.gl_product_group_desc,
        t3.order_date,
        t3.brand_name,
        t3.brand_code,
        t3.product_category_desc,
        p.promotion_type,
        p.promo_start_date,
        p.promo_end_date,
        promo_duration_days,
        promo_yr,
        is_deal_purchase,
        event_name
);


DROP TABLE IF EXISTS promo_lift;    
CREATE TEMP TABLE promo_lift AS (
    WITH deal_periods AS (
        SELECT DISTINCT 
            acp.asin,
            acp.promo_start_date as deal_start_date,
            acp.promo_end_date as deal_end_date,
            acp.event_name,
            acp.promo_duration_days
        FROM asin_promos_cp acp
        WHERE acp.is_deal_purchase = 'Y'
    ),

    pre_deal_metrics AS (
        SELECT 
            acp.asin,
            dp.deal_start_date,
            dp.event_name,
            -- Sum metrics for 14-day period starting 28 days before deal
            SUM(acp.shipped_units) as pre_deal_units,
            SUM(acp.revenue_share_amt) as pre_deal_revenue,
            SUM(acp.display_ads_amt) as pre_deal_display_ads,
            SUM(acp.subscription_revenue_amt) as pre_deal_subscription,
            SUM(acp.first_purchase_customers) as pre_deal_new_customers,
            SUM(acp.one_mo_return_customers) as pre_deal_1mo_returns,
            SUM(acp.two_mo_return_customers) as pre_deal_2mo_returns,
            SUM(acp.three_mo_return_customers) as pre_deal_3mo_returns
        FROM asin_promos_cp acp
        JOIN deal_periods dp 
            ON acp.asin = dp.asin
            AND acp.order_date BETWEEN 
                (dp.deal_start_date - interval '28 day') 
                AND 
                (dp.deal_start_date - interval '14 day')
        GROUP BY 
            acp.asin,
            dp.deal_start_date,
            dp.event_name
    ),

    deal_metrics AS (
        SELECT 
            acp.asin,
            acp.promo_start_date,
            acp.event_name,
            acp.gl_product_group_desc,
            acp.brand_name,
            acp.product_category_desc,
            dp.promo_duration_days as deal_duration,
            dp.deal_start_date,
            dp.deal_end_date,
            SUM(acp.shipped_units) as total_deal_units,
            SUM(acp.revenue_share_amt) as total_deal_revenue,
            SUM(acp.display_ads_amt) as total_deal_display_ads,
            SUM(acp.subscription_revenue_amt) as total_deal_subscription,
            SUM(acp.first_purchase_customers) as total_deal_new_customers,
            SUM(acp.one_mo_return_customers) as total_deal_1mo_returns,
            SUM(acp.two_mo_return_customers) as total_deal_2mo_returns,
            SUM(acp.three_mo_return_customers) as total_deal_3mo_returns
        FROM asin_promos_cp acp
        JOIN deal_periods dp 
            ON acp.asin = dp.asin
            AND acp.order_date BETWEEN dp.deal_start_date AND dp.deal_end_date
        WHERE acp.is_deal_purchase = 'Y'
        GROUP BY 
            acp.asin,
            acp.promo_start_date,
            acp.event_name,
            acp.gl_product_group_desc,
            acp.brand_name,
            acp.product_category_desc,
            dp.promo_duration_days,
            dp.deal_start_date,
            dp.deal_end_date
    )

    SELECT 
        dm.asin,
        dm.gl_product_group_desc,
        dm.brand_name,
        dm.product_category_desc,
        dm.event_name,
        dm.promo_start_date,
        dm.deal_duration,
        dm.deal_start_date,
        dm.deal_end_date,
        
        -- Deal period metrics
        dm.total_deal_units,
        dm.total_deal_revenue,
        dm.total_deal_display_ads,
        dm.total_deal_subscription,
        dm.total_deal_new_customers,
        dm.total_deal_1mo_returns,
        dm.total_deal_2mo_returns,
        dm.total_deal_3mo_returns,
        
        -- Pre-deal period metrics (exact same duration as deal period)
        COALESCE(pdm.pre_deal_units, 0) as pre_deal_units,
        COALESCE(pdm.pre_deal_revenue, 0) as pre_deal_revenue,
        COALESCE(pdm.pre_deal_display_ads, 0) as pre_deal_display_ads,
        COALESCE(pdm.pre_deal_subscription, 0) as pre_deal_subscription,
        COALESCE(pdm.pre_deal_new_customers, 0) as pre_deal_new_customers,
        COALESCE(pdm.pre_deal_1mo_returns, 0) as pre_deal_1mo_returns,
        COALESCE(pdm.pre_deal_2mo_returns, 0) as pre_deal_2mo_returns,
        COALESCE(pdm.pre_deal_3mo_returns, 0) as pre_deal_3mo_returns,
        
        -- Calculate percentage changes
        CASE 
            WHEN COALESCE(pdm.pre_deal_units, 0) = 0 THEN NULL
            ELSE ROUND(((dm.total_deal_units - pdm.pre_deal_units) / pdm.pre_deal_units) * 100, 2)
        END as units_pct_change,
        
        CASE 
            WHEN COALESCE(pdm.pre_deal_revenue, 0) = 0 THEN NULL
            ELSE ROUND(((dm.total_deal_revenue - pdm.pre_deal_revenue) / pdm.pre_deal_revenue) * 100, 2)
        END as revenue_pct_change,
        
        CASE 
            WHEN COALESCE(pdm.pre_deal_display_ads, 0) = 0 THEN NULL
            ELSE ROUND(((dm.total_deal_display_ads - pdm.pre_deal_display_ads) / pdm.pre_deal_display_ads) * 100, 2)
        END as display_ads_pct_change,
        
        CASE 
            WHEN COALESCE(pdm.pre_deal_new_customers, 0) = 0 THEN NULL
            ELSE ROUND(((dm.total_deal_new_customers - pdm.pre_deal_new_customers) / pdm.pre_deal_new_customers) * 100, 2)
        END as new_customers_pct_change

    FROM deal_metrics dm
    LEFT JOIN pre_deal_metrics pdm
        ON dm.asin = pdm.asin
        AND dm.promo_start_date = pdm.promo_start_date
        AND dm.event_name = pdm.event_name
    ORDER BY 
        dm.promo_start_date DESC,
        dm.total_deal_revenue DESC
);
 

 -- Company code

-- -- Create table for promotional metrics
-- DROP TABLE IF EXISTS asin_promo_metrics;    
-- CREATE TEMP TABLE asin_promo_metrics AS (
--     SELECT   
--         marketplace_id as marketplace_key,
--         asin,
--         promotion_type,
--         promo_yr,
--         promo_mo,
--         start_datetime,
--         duration_days, 
--         is_deal_purchase,
--         event_name,
--         gl_product_group_desc,
--         brand_name,
--         brand_code,
--         product_category_desc,
--         COUNT(DISTINCT customer_id)/NULLIF(duration_days, 0) as promo_daily_unique_customers,
--         SUM(first_purchase_flag)/NULLIF(duration_days, 0) as promo_daily_new_customers,
--         SUM(one_mo_return_flag + two_mo_return_flag + 
--             third_mo_return_flag + three_mo_plus_return_flag)/NULLIF(duration_days, 0) as promo_daily_total_return_customers,
--         SUM(shipped_units)/NULLIF(duration_days, 0) as promo_daily_units_sold,
--         SUM(revenue_share_amt)/NULLIF(duration_days, 0) as promo_daily_revenue
--     FROM asin_promos_cp
--     WHERE is_deal_purchase = 'Y'
--     --     --     -- AND t2.asin_approval_status='Approved'
-- --         -- AND t2.suppression_reason = '' or t2.suppression_reason = '[]' --test if this works
-- --         -- AND t4.paws_promotion_id is not null
-- --         -- AND t4.suppression_state!='Fully Suppressed'
-- --         -- AND t4.approval_status='Approved'
--     GROUP BY 
--         marketplace_id,
--         asin,
--         promotion_type,
--         yr,
--         start_datetime,
--         duration_days,
--         is_deal_purchase,
--         event_name,
--         gl_product_group_desc,
--         brand_name,
--         brand_code,
--         product_category_desc
-- );

-- --pre-promotion ASINs
-- DROP TABLE IF EXISTS asin_nonpromo_metrics;
-- CREATE TEMP TABLE asin_nonpromo_metrics AS (
--   SELECT
--     marketplace_id as marketplace_key,
--     asin,
--     gl_product_group_desc,
--     brand_name,
--     brand_code, 
--     product_category_desc,
--     order_date,
--     COUNT(DISTINCT customer_id) as regular_unique_customers,
--     SUM(first_purchase_flag) as regular_new_customers,
--     SUM(one_mo_return_flag + two_mo_return_flag +
--         third_mo_return_flag + three_mo_plus_return_flag) as regular_daily_total_return_customers,
--     SUM(shipped_units) as regular_units_sold,
--     SUM(revenue_share_amt) as regular_revenue
--   FROM asin_promos_cp
--   WHERE is_deal_purchase = 'N'
--   GROUP BY 1,2,3,4,5,6,7
-- );

-- -- Compare promotional vs non-promotional performance
-- DROP TABLE IF EXISTS asin_event_comparison;
-- CREATE TEMP TABLE asin_event_comparison AS (
--   SELECT
--       p.marketplace_key,
--       p.asin,
--       p.promotion_type,
--       p.promo_yr,
--       p.promo_mo,
--       p.duration_days,
--       p.event_name,
--       p.gl_product_group_desc,
--       p.brand_name,
--       -- Promotional metrics
--       p.unique_customers as promo_daily_customers,
--       p.new_customers as promo_daily_new_buyers,
--       p.units_sold as promo_daily_units,
--       p.revenue as promo_daily_revenue,
--       p.total_return_customers as promo_daily_return_customers,
--       -- Non-promotional metrics (22 days before promotion) 
--       SUM(COALESCE(n.unique_customers,0))/ 21 as regular_daily_uniq_customers,
--       SUM(n.new_customers)/21 as regular_daily_new_buyers,
--       SUM(n.total_returns)/21 as regular_daily_return_customers,
--       SUM(n.units_sold)/21 as regular_daily_units,
--       SUM(n.revenue)/21 as regular_daily_revenue
--   FROM asin_promo_metrics p
--       LEFT JOIN asin_nonpromo_metrics n
--         ON p.marketplace_key = n.marketplace_key
--         AND p.asin = n.asin
--         AND p.gl_product_group_desc = n.gl_product_group_desc
--         AND p.brand_name = n.brand_name
--         AND n.order_date BETWEEN p.start_datetime - INTERVAL '42 days' 
--                             AND p.start_datetime - INTERVAL '22 days' 
--   WHERE p.start_datetime + INTERVAL '365 days' >= TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
--   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
-- );



DROP TABLE IF EXISTS category_benchmark;
CREATE TEMP TABLE category_benchmark as (
    select
        t1.marketplace_id,
        t1.gl_product_group_desc,
        t1.product_category_desc,
        t1.is_deal_purchase,
        count(distinct t1.customer_id) as unique_customer_ct,
        sum(t1.first_purchase_ct) as first_purchase_ct,
        sum(t1.one_mo_return_ct) as one_mo_return_ct, 
        sum(t1.two_mo_return_ct) as two_mo_return_ct,
        sum(t1.third_mo_return_ct) as third_mo_return_ct,
        sum(t1.shipped_units) as shipped_units,
        sum(t1.revenue_share_amt) as revenue_share_amt,
        sum(t1.display_ads_amt) as display_ads_amt,
        sum(t1.subscription_revenue_amt) as subscription_revenue_amt,
        sum(t1.coupon_quantity) as promo_coupon_quantity
    from asin_purchase t1
    group by 
        t1.marketplace_id,
        t1.gl_product_group_desc,
        t1.is_deal_purchase,
        t1.product_category_desc
);

DROP TABLE IF EXISTS gl_benchmark;
CREATE TEMP TABLE gl_benchmark as (
    select
        t1.marketplace_id,
        t1.gl_product_group_desc,
        t1.is_deal_purchase,
        count(distinct t1.customer_id) as unique_customer_ct,
        sum(t1.first_purchase_ct) as first_purchase_ct,
        sum(t1.one_mo_return_ct) as one_mo_return_ct, 
        sum(t1.two_mo_return_ct) as two_mo_return_ct,
        sum(t1.third_mo_return_ct) as third_mo_return_ct,
        sum(t1.shipped_units) as shipped_units,
        sum(t1.revenue_share_amt) as revenue_share_amt,
        sum(t1.display_ads_amt) as display_ads_amt,
        sum(t1.subscription_revenue_amt) as subscription_revenue_amt,
        sum(t1.coupon_quantity) as promo_coupon_quantity
    from asin_purchase t1
    group by 
        t1.marketplace_id,
        t1.gl_product_group_desc,
        t1.is_deal_purchase
        -- t1.product_category_desc
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

-------------------------------------------------- OUTPUT --------------------------------------------------
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