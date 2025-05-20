/* 
Base Orders Query
- Gets order data for consumables categories
- Includes only retail merchant orders with shipped units > 0 
*/
DROP TABLE IF EXISTS base_orders;
CREATE TEMP TABLE base_orders AS (
    SELECT 
        o.asin,
        o.customer_id,
        o.customer_shipment_item_id,
        TO_DATE(o.order_datetime, 'YYYY-MM-DD') as order_date,
        o.shipped_units,
        maa.gl_product_group,
        maa.brand_name,
        maa.brand_code,
        COALESCE(cp.revenue_share_amt, 0) as revenue_share_amt,
        COALESCE(cp.display_ads_amt, 0) as display_ads_amt,
        COALESCE(cp.subscription_revenue_amt, 0) as subscription_revenue_amt
    FROM andes.booker.d_unified_cust_shipment_items o
        INNER JOIN andes.booker.d_mp_asin_attributes maa
            ON maa.asin = o.asin
            AND maa.marketplace_id = o.marketplace_id
            AND maa.region_id = o.region_id
            AND maa.gl_product_group IN (510, 364, 325, 199, 194, 121, 75)
        LEFT JOIN andes.contribution_ddl.o_wbr_cp_na cp
            ON o.customer_shipment_item_id = cp.customer_shipment_item_id 
            AND o.asin = cp.asin
    WHERE o.region_id = 1
        AND o.marketplace_id = 7
        AND o.shipped_units > 0
        AND o.is_retail_merchant = 'Y'
        AND o.order_datetime BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '386 days'
            AND TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
);


/*
Promotion Details
- Classifies promotions into major event types
*/
DROP TABLE IF EXISTS promotion_details;
CREATE TEMP TABLE promotion_details AS (
    SELECT DISTINCT
        f.customer_shipment_item_id,
        f.asin,
        p.promotion_key,
        TO_DATE(p.start_datetime, 'YYYY-MM-DD') as promo_start_date,
        TO_DATE(p.end_datetime, 'YYYY-MM-DD') as promo_end_date,
        CASE 
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
        END as event_name
    FROM andes.pdm.fact_promotion_cp f
        JOIN andes.pdm.dim_promotion p
        ON f.promotion_key = p.promotion_key
        AND p.marketplace_key = 7
    WHERE f.region_id = 1
        AND f.marketplace_key = 7
);


/*
Customer Purchase History
- Identifies first time purchases and return purchase patterns per customer per brand
*/
DROP TABLE IF EXISTS customer_history;
CREATE TEMP TABLE customer_history AS (
    SELECT 
        customer_id,
        brand_code,
        brand_name,
        order_date,
        CASE 
            WHEN LAG(order_date) OVER (
                PARTITION BY customer_id, brand_code 
                ORDER BY order_date
            ) IS NULL THEN 1
            ELSE 0
        END as is_first_brand_purchase,
        CASE
            WHEN order_date - LAG(order_date) OVER (
                PARTITION BY customer_id, brand_code 
                ORDER BY order_date
            ) <= interval '30 days' THEN 1
            ELSE 0
        END as is_one_month_return,
        CASE
            WHEN order_date - LAG(order_date) OVER (
                PARTITION BY customer_id, brand_code 
                ORDER BY order_date
            ) BETWEEN interval '31 days' AND interval '60 days' THEN 1
            ELSE 0
        END as is_two_month_return,
        CASE
            WHEN order_date - LAG(order_date) OVER (
                PARTITION BY customer_id, brand_code 
                ORDER BY order_date
            ) BETWEEN interval '61 days' AND interval '90 days' THEN 1
            ELSE 0
        END as is_three_month_return,
        CASE
            WHEN order_date - LAG(order_date) OVER (
                PARTITION BY customer_id, brand_code 
                ORDER BY order_date
            ) > interval '90 days' THEN 1
            ELSE 0
        END as is_three_plus_month_return
    FROM (
        -- Deduplicate multiple purchases on same day
        SELECT DISTINCT
            customer_id,
            brand_code,
            brand_name,
            order_date
        FROM base_orders
        WHERE brand_code IS NOT NULL
    )
);


/*
Combine Promo & Non-Promo Orders with Promotions Data
*/
DROP TABLE IF EXISTS orders_with_promos;
CREATE TEMP TABLE orders_with_promos AS (

    WITH promo_dates AS (
        SELECT DISTINCT
            asin,
            promo_start_date,
            promo_end_date,
            event_name
        FROM promotion_details
    )

    SELECT 
        b.*,
        p.promo_start_date,
        p.promo_end_date,
        p.event_name,
        CASE 
            WHEN b.order_date BETWEEN p.promo_start_date AND p.promo_end_date THEN 'Y'
            ELSE 'N'
        END as is_promotion,
        CASE
            WHEN p.promo_start_date > p.promo_end_date THEN 'INVALID_PROMO_DATES'
            WHEN b.order_date BETWEEN p.promo_start_date AND p.promo_end_date THEN 'DEAL'
            WHEN b.order_date BETWEEN p.promo_start_date - interval '28 days' AND p.promo_start_date - interval '14 days' THEN 'PRE_DEAL'
            WHEN p.asin IS NULL THEN 'NOT_DEAL'
            ELSE 'OTHER'
        END as period_type
    FROM base_orders b
        LEFT JOIN promo_dates p 
            ON b.asin = p.asin
            AND b.order_date BETWEEN p.promo_start_date - interval '28 days' AND p.promo_end_date
);


/*
Promotion Analysis
*/
DROP TABLE IF EXISTS promotion_analysis;
CREATE TEMP TABLE promotion_analysis AS (
    SELECT 
        o.asin,
        o.gl_product_group,
        o.brand_name,
        o.brand_code,
        o.event_name,
        o.promo_start_date,
        o.promo_end_date,
        o.order_date,
        o.period_type,
        SUM(o.shipped_units) as units,
        SUM(o.revenue_share_amt) as revenue,
        SUM(o.display_ads_amt) as display_ads,
        SUM(o.subscription_revenue_amt) as subscription_revenue,
        COUNT(DISTINCT o.customer_id) as total_customers,
        SUM(COALESCE(h.is_first_brand_purchase, 0)) as new_to_brand_customers,
        SUM(COALESCE(h.is_one_month_return, 0)) as one_mo_return_customers,
        SUM(COALESCE(h.is_two_month_return, 0)) as two_mo_return_customers,
        SUM(COALESCE(h.is_three_month_return, 0)) as three_mo_return_customers,
        SUM(COALESCE(h.is_three_plus_month_return, 0)) as three_plus_mo_return_customers
    FROM orders_with_promos o
        LEFT JOIN customer_history h
        ON o.customer_id = h.customer_id 
        AND o.brand_code = h.brand_code
        AND o.order_date = h.order_date
    WHERE o.order_date BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '385 days'
        AND TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
    GROUP BY 
        o.asin,
        o.gl_product_group,
        o.brand_name,
        o.brand_code,
        o.event_name,
        o.promo_start_date,
        o.promo_end_date,
        o.order_date,
        o.period_type
);


/*
Final Analysis
*/
DROP TABLE IF EXISTS final_output;
CREATE TEMP TABLE final_output AS (
    
    WITH deal_periods AS (
        SELECT 
            p.asin,
            p.gl_product_group,
            p.brand_name,
            p.brand_code,
            p.promo_start_date,
            p.promo_end_date,
            p.event_name,
            -- p.promo_end_date - p.promo_start_date + 1
            DATEDIFF('day', p.promo_start_date, p.promo_end_date) + 1 as deal_duration_days,
            SUM(CASE WHEN p.period_type = 'DEAL' THEN p.units ELSE 0 END) as deal_units,
            SUM(CASE WHEN p.period_type = 'DEAL' THEN p.revenue ELSE 0 END) as deal_revenue,
            SUM(CASE WHEN p.period_type = 'DEAL' THEN p.new_to_brand_customers ELSE 0 END) as deal_new_to_brand,
            SUM(CASE WHEN p.period_type = 'DEAL' THEN p.one_mo_return_customers ELSE 0 END) as deal_one_mo_return,
            SUM(CASE WHEN p.period_type = 'DEAL' THEN p.two_mo_return_customers ELSE 0 END) as deal_two_mo_return,
            SUM(CASE WHEN p.period_type = 'DEAL' THEN p.three_mo_return_customers ELSE 0 END) as deal_three_mo_return,
            SUM(CASE WHEN p.period_type = 'DEAL' THEN p.three_plus_mo_return_customers ELSE 0 END) as deal_three_plus_mo_return
        FROM promotion_analysis p
        WHERE p.period_type = 'DEAL'
        GROUP BY 
            p.asin,
            p.gl_product_group,
            p.brand_name,
            p.brand_code,
            p.promo_start_date,
            p.promo_end_date,
            p.event_name
    ),

    pre_deal_metrics AS (
        SELECT 
            d.asin,
            d.brand_code,
            d.promo_start_date,
            d.promo_end_date,  -- Add this line
            d.event_name,
            d.gl_product_group,
            d.brand_name,
            d.deal_duration_days,
            
            -- Pre-deal aggregations
            SUM(CASE WHEN p.period_type = 'PRE_DEAL' THEN p.units ELSE 0 END) as pre_deal_base_units,
            SUM(CASE WHEN p.period_type = 'PRE_DEAL' THEN p.revenue ELSE 0 END) as pre_deal_base_revenue,
            SUM(CASE WHEN p.period_type = 'PRE_DEAL' THEN p.new_to_brand_customers ELSE 0 END) as pre_deal_base_new_to_brand,
            SUM(CASE WHEN p.period_type = 'PRE_DEAL' THEN p.one_mo_return_customers ELSE 0 END) as pre_deal_base_one_mo_return,
            SUM(CASE WHEN p.period_type = 'PRE_DEAL' THEN p.two_mo_return_customers ELSE 0 END) as pre_deal_base_two_mo_return,
            SUM(CASE WHEN p.period_type = 'PRE_DEAL' THEN p.three_mo_return_customers ELSE 0 END) as pre_deal_base_three_mo_return,
            SUM(CASE WHEN p.period_type = 'PRE_DEAL' THEN p.three_plus_mo_return_customers ELSE 0 END) as pre_deal_base_three_plus_mo_return,

            -- Deal metrics (already aggregated from deal_periods CTE)
            d.deal_units,
            d.deal_revenue,
            d.deal_new_to_brand,
            d.deal_one_mo_return,
            d.deal_two_mo_return,
            d.deal_three_mo_return,
            d.deal_three_plus_mo_return

        FROM deal_periods d
            LEFT JOIN promotion_analysis p
            ON d.asin = p.asin
            AND d.brand_code = p.brand_code
            AND p.order_date BETWEEN d.promo_start_date - interval '28 days' 
                AND d.promo_start_date - interval '14 days'
        GROUP BY 
            d.asin,
            d.brand_code,
            d.promo_start_date,
            d.promo_end_date,  -- Add this line
            d.event_name,
            d.gl_product_group,
            d.brand_name,
            d.deal_duration_days,
            d.deal_units,
            d.deal_revenue,
            d.deal_new_to_brand,
            d.deal_one_mo_return,
            d.deal_two_mo_return,
            d.deal_three_mo_return,
            d.deal_three_plus_mo_return
    )

    SELECT 
        asin,
        gl_product_group,
        brand_name,
        event_name,
        promo_start_date,
        promo_end_date,
        deal_duration_days,
        
        -- Deal metrics
        deal_units,
        deal_revenue,
        deal_new_to_brand,
        deal_one_mo_return,
        deal_two_mo_return,
        deal_three_mo_return,
        deal_three_plus_mo_return,
        
        -- Pre-deal projected metrics (scale up from 14-day base)
        ROUND((pre_deal_base_units::FLOAT / 14) * deal_duration_days, 0) as projected_pre_deal_units,
        ROUND((pre_deal_base_revenue::FLOAT / 14) * deal_duration_days, 2) as projected_pre_deal_revenue,
        ROUND((pre_deal_base_new_to_brand::FLOAT / 14) * deal_duration_days, 0) as projected_pre_deal_new_to_brand,
        ROUND((pre_deal_base_one_mo_return::FLOAT / 14) * deal_duration_days, 0) as projected_pre_deal_one_mo_return,
        ROUND((pre_deal_base_two_mo_return::FLOAT / 14) * deal_duration_days, 0) as projected_pre_deal_two_mo_return,
        ROUND((pre_deal_base_three_mo_return::FLOAT / 14) * deal_duration_days, 0) as projected_pre_deal_three_mo_return,
        ROUND((pre_deal_base_three_plus_mo_return::FLOAT / 14) * deal_duration_days, 0) as projected_pre_deal_three_plus_mo_return,
        
        -- Percentage changes
        ROUND(CASE 
            WHEN projected_pre_deal_units = 0 THEN NULL
            ELSE ((deal_units::FLOAT - projected_pre_deal_units) * 100.0 / projected_pre_deal_units)
        END, 2) as units_pct_change,
        
        ROUND(CASE 
            WHEN projected_pre_deal_revenue = 0 THEN NULL
            ELSE ((deal_revenue::FLOAT - projected_pre_deal_revenue) * 100.0 / projected_pre_deal_revenue)
        END, 2) as revenue_pct_change

    FROM pre_deal_metrics
    ORDER BY 
        promo_start_date DESC,
        deal_revenue DESC
);


select * from final_output;