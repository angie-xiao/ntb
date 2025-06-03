/*************************
Base Orders Query
- Gets order data for consumables categories
- Includes only retail merchant orders with shipped units > 0 
*************************/
DROP TABLE IF EXISTS base_orders;
CREATE TEMP TABLE base_orders AS (
    SELECT 
        o.asin,
        maa.item_name,
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
        AND o.order_datetime BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '456 days' -- 365 + t13w (91 days)
            AND TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
);


/*************************
Promotion Details
- Classifies promotions into major event types
https://w.amazon.com/bin/view/Canada_Marketing/Events/2025_Events/
*************************/
DROP TABLE IF EXISTS promotion_details;
CREATE TEMP TABLE promotion_details AS (
    SELECT DISTINCT
        f.customer_shipment_item_id,
        f.asin,
        p.promotion_key,
        TO_DATE(p.start_datetime, 'YYYY-MM-DD') as promo_start_date,
        TO_DATE(p.end_datetime, 'YYYY-MM-DD') as promo_end_date,
        p.promotion_internal_title,
        (CASE 
            WHEN p.promotion_key IS NULL THEN 'NO_PROMOTION'

            -- tier 1
            WHEN UPPER(p.promotion_internal_title) LIKE '%BSS%' 
                OR UPPER(p.promotion_internal_title) LIKE '%BIG SPRING SALE%' THEN 'BSS'
            WHEN UPPER(p.promotion_internal_title) LIKE '%PRIME%DAY%'
                OR UPPER(p.promotion_internal_title) LIKE '%PD%' THEN 'PRIME DAY'
            WHEN UPPER(p.promotion_internal_title) LIKE '%PBDD%' THEN 'PBDD'
            WHEN UPPER(p.promotion_internal_title) LIKE '%BF%'
                OR UPPER(p.promotion_internal_title) LIKE '%BLACK%FRIDAY%' THEN 'BLACK FRIDAY'
            WHEN UPPER(p.promotion_internal_title) LIKE '%CYBER%MONDAY%'
                OR UPPER(p.promotion_internal_title) LIKE '%CM%' THEN 'CYBER MONDAY'
            WHEN UPPER(p.promotion_internal_title) LIKE '%BOXING WEEK%'
                OR UPPER(p.promotion_internal_title) LIKE '%BOXING DAY%' THEN 'BOXING WEEK'
            WHEN UPPER(p.promotion_internal_title) LIKE '%T5%'
                OR UPPER(p.promotion_internal_title) LIKE '%T11%'
                OR UPPER(p.promotion_internal_title) LIKE '%T12%' THEN 'T5/11/12'

            -- tier 1.5
            WHEN UPPER(p.promotion_internal_title) LIKE '%BACK%TO%SCHOOL%' THEN 'BACK TO SCHOOL' 
            WHEN UPPER(p.promotion_internal_title) LIKE '%BACK%TO%UNIVERSITY%' THEN 'BACK TO UNIVERSITY' 

            -- tier 2
            WHEN UPPER(p.promotion_internal_title) LIKE '%NYNY%' THEN 'NYNY'

            WHEN UPPER(p.promotion_internal_title) LIKE '%MOTHER%DAY%' 
                OR UPPER(p.promotion_internal_title) LIKE '%MOTHERS%DAY%' 
                OR UPPER(p.promotion_internal_title) LIKE '%MOTHER_S%DAY%'
                OR UPPER(p.promotion_internal_title) LIKE '%MOTHER''''S%DAY%' THEN 'MOTHERS DAY'
            WHEN UPPER(p.promotion_internal_title) LIKE '%FATHER%DAY%' 
                OR UPPER(p.promotion_internal_title) LIKE '%FATHERS%DAY%' 
                OR UPPER(p.promotion_internal_title) LIKE '%FATHER_S%DAY%'
                OR UPPER(p.promotion_internal_title) LIKE '%FATHER''''S%DAY%' THEN 'FATHERS DAY'
            WHEN UPPER(p.promotion_internal_title) LIKE '%VALENTINE%DAY%' 
                OR UPPER(p.promotion_internal_title) LIKE '%VALENTINES%DAY%'
                OR UPPER(p.promotion_internal_title) LIKE '%VALENTINE_S%DAY%'
                OR UPPER(p.promotion_internal_title) LIKE '%VALENTINE''''S%DAY%' THEN 'VALENTINES DAY'
            WHEN UPPER(p.promotion_internal_title) LIKE '%GIFTMANIA%' 
                OR UPPER(p.promotion_internal_title) LIKE '%GIFT%MANIA%' THEN 'GIFT MANIA'
            WHEN UPPER(p.promotion_internal_title) LIKE '%HALLOWEEN%' THEN 'HALLOWEEN'
            WHEN UPPER(p.promotion_internal_title) LIKE '%HOLIDAY%' THEN 'HOLIDAY'
            
            -- tier 3    
            WHEN UPPER(p.promotion_internal_title) LIKE '%LUNAR%NEW%YEAR%' THEN 'LUNAR NEW YEAR'
            WHEN UPPER(p.promotion_internal_title) LIKE '%DAILY%ESSENTIALS%' THEN 'DAILY ESSENTIALS'
            WHEN UPPER(p.promotion_internal_title) LIKE '%BEAUTY%HAUL%' THEN 'BEAUTY HAUL'
            WHEN UPPER(p.promotion_internal_title) LIKE '%PET%DAY%' 
                OR UPPER(p.promotion_internal_title) LIKE '%PET%MONTH%' THEN 'PET DAY'
            WHEN UPPER(p.promotion_internal_title) LIKE '%HEALTH%WELLNESS%' THEN 'HEALTH & WELLNESS MONTH'
            WHEN UPPER(p.promotion_internal_title) LIKE '%GAMING%MONTH%' THEN 'GAMING MONTH'
            WHEN UPPER(p.promotion_internal_title) LIKE '%BABY%SAVINGS%' THEN 'BABY SAVINGS'
            WHEN UPPER(p.promotion_internal_title) LIKE '%BEAUTY%WEEK%' THEN 'BEAUTY WEEK'
            WHEN UPPER(p.promotion_internal_title) LIKE '%DIWALI%' THEN 'DIWALI'
            WHEN UPPER(p.promotion_internal_title) LIKE '%MOVEMBER%' THEN 'MOVEMBER'
            WHEN UPPER(p.promotion_internal_title) LIKE '%FLASH SALE%' THEN 'FLASH SALE'
            ELSE 'OTHER'
        END) as event_name
    FROM andes.pdm.fact_promotion_cp f
        JOIN andes.pdm.dim_promotion p
        ON f.promotion_key = p.promotion_key
        AND p.marketplace_key = 7
    WHERE f.region_id = 1
        AND f.marketplace_key = 7
        AND (p.approval_status = 'Approved' or p.approval_status = 'Scheduled')
        AND p.promotion_type in (
            'Best Deal', 'Deal of the Day', 'Lightning Deal', 'Event Deal'
            -- 'Local Deal', Price Discount, Sales Discount, Coupon, Subscribe & Save, Markdown, Content Only, Pegasus Campaign, Gemini Campaign
        )
        -- AND p.promotion_type NOT IN ('Coupon', 'Sales Discount')
        AND UPPER(p.promotion_internal_title) NOT LIKE '%OIH%'
        AND TO_DATE(p.start_datetime, 'YYYY-MM-DD') BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '365 days'
            AND TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
);


/*************************
Create a consolidated promotions table to handle overlapping periods
*************************/
DROP TABLE IF EXISTS consolidated_promos;
CREATE TEMP TABLE consolidated_promos AS (
    WITH overlapping_periods AS (
        SELECT 
            asin,
            customer_shipment_item_id,  -- Add this to track which transactions belong to which promo
            event_name,
            DATE_PART('year', promo_start_date) as event_year, 
            DATE_PART('month', promo_start_date) as event_month,
            MIN(promo_start_date) as start_date,
            MAX(promo_end_date) as end_date
        FROM promotion_details
        GROUP BY 
            asin,
            customer_shipment_item_id,  -- Group by transaction to avoid double counting
            event_name,
            DATE_PART('year', promo_start_date),
            DATE_PART('month', promo_start_date)
    )
    SELECT DISTINCT
        asin,
        customer_shipment_item_id,
        event_name,
        event_year,
        event_month,
        start_date as promo_start_date,
        end_date as promo_end_date
    FROM overlapping_periods
);


/*************************
Deal period orders
*************************/
DROP TABLE IF EXISTS deal_orders;
CREATE TEMP TABLE deal_orders AS (
    SELECT 
        b.*,
        p.promo_start_date,
        p.promo_end_date,
        p.event_name,
        p.event_year,
        p.event_month,
        (CASE 
            WHEN p.promo_end_date >= TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') 
                THEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - p.promo_start_date + 1
            ELSE p.promo_end_date - p.promo_start_date + 1
        END) as event_duration_days,
        'DEAL' as period_type,
        'Y' as is_promotion
    FROM base_orders b
        INNER JOIN consolidated_promos p 
        ON b.asin = p.asin
        AND b.customer_shipment_item_id = p.customer_shipment_item_id
        AND b.order_date BETWEEN p.promo_start_date AND p.promo_end_date
);


/*************************
Create pre-deal date ranges

Pre-deal range = T13W
*************************/
DROP TABLE IF EXISTS pre_deal_date_ranges;
CREATE TEMP TABLE pre_deal_date_ranges AS (
    SELECT 
        asin,
        -- item_name,
        promo_start_date,
        promo_end_date,
        event_name,
        event_year,
        event_month,
        promo_start_date - interval '91 day' AS pre_deal_start_date,
        promo_start_date - interval '1 day' AS pre_deal_end_date -- T13W
    FROM consolidated_promos
);


/*************************
Pre-Deal period orders with clean joins
*************************/
DROP TABLE IF EXISTS pre_deal_orders;
CREATE TEMP TABLE pre_deal_orders AS (
    SELECT DISTINCT
        b.*,
        pdr.promo_start_date,
        pdr.promo_end_date,
        pdr.event_name,
        pdr.event_year,
        pdr.event_month,
        'PRE_DEAL' as period_type,
        'N' as is_promotion
    FROM base_orders b
        INNER JOIN pre_deal_date_ranges pdr
        ON b.asin = pdr.asin
    WHERE b.order_date BETWEEN pdr.pre_deal_start_date AND pdr.pre_deal_end_date

);


/*************************
FIRST PURCHASE - Single source of truth
*************************/
DROP TABLE IF EXISTS first_purchases;
CREATE TEMP TABLE first_purchases AS (
    SELECT 
        customer_id,
        brand_code,
        MIN(order_date) as first_purchase_date
    FROM (
        SELECT customer_id, brand_code, order_date FROM deal_orders
        UNION ALL
        SELECT customer_id, brand_code, order_date FROM pre_deal_orders
    ) all_orders
    WHERE brand_code IS NOT NULL
    GROUP BY customer_id, brand_code
);


/*************************
Per customer per day summary
Split by period type but using same first_purchases
*************************/
DROP TABLE IF EXISTS deal_daily_summary;
CREATE TEMP TABLE deal_daily_summary AS (
    SELECT 
        o.asin,
        o.item_name,
        o.gl_product_group,
        o.brand_code,
        o.brand_name,
        o.event_name,
        o.promo_start_date,
        o.promo_end_date,
        o.event_duration_days,
        o.period_type,
        o.order_date,
        o.customer_id,
        (CASE 
            WHEN o.order_date = fp.first_purchase_date THEN 1 
            ELSE 0 
        END) AS is_first_brand_purchase,
        o.shipped_units,
        o.revenue_share_amt,
        o.display_ads_amt,
        o.subscription_revenue_amt
    FROM deal_orders o
        LEFT JOIN first_purchases fp
        ON o.customer_id = fp.customer_id
        AND o.brand_code = fp.brand_code
);

DROP TABLE IF EXISTS pre_deal_daily_summary;
CREATE TEMP TABLE pre_deal_daily_summary AS (
    SELECT 
        o.asin,
        o.item_name,
        o.gl_product_group,
        o.brand_code,
        o.brand_name,
        o.event_name,
        o.promo_start_date,
        o.promo_end_date,
        o.period_type,
        o.order_date,
        o.customer_id,
        (CASE 
            WHEN o.order_date = fp.first_purchase_date THEN 1 
            ELSE 0 
        END) AS is_first_brand_purchase,
        o.shipped_units,
        o.revenue_share_amt,
        o.display_ads_amt, 
        o.subscription_revenue_amt
    FROM pre_deal_orders o
        LEFT JOIN first_purchases fp
        ON o.customer_id = fp.customer_id
        AND o.brand_code = fp.brand_code
);


/*************************
Deal metrics calculation (daily avg)
*************************/
DROP TABLE IF EXISTS deal_metrics;
CREATE TEMP TABLE deal_metrics AS (
    WITH base_metrics AS (
        SELECT 
            asin,
            item_name,
            gl_product_group,
            brand_code,
            brand_name,
            event_name,
            promo_start_date,
            promo_end_date,
            DATE_PART('month', promo_start_date) as event_month,
            DATE_PART('year', promo_start_date) as event_year,
            (CASE 
                WHEN promo_end_date >= TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') 
                    THEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - promo_start_date + 1
                ELSE promo_end_date - promo_start_date + 1
            END) as event_duration_days,
            -- total
            SUM(shipped_units) as shipped_units,
            SUM(revenue_share_amt) as revenue,
            SUM(display_ads_amt) as display_ads_amt,
            SUM(subscription_revenue_amt) as subscription_revenue_amt,
            COUNT(DISTINCT customer_id) as total_customers,
            COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END) as new_customers,
            COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END) as return_customers
        FROM deal_daily_summary
        -- Add grouping by event context
        GROUP BY 1,2,3,4,5,6,7,8
    )
    SELECT 
        bm.*,
        mam.dama_mfg_vendor_code as vendor_code,
        -- daily
        shipped_units/event_duration_days as daily_deal_shipped_units,
        revenue/event_duration_days as daily_deal_ops,            
        display_ads_amt/event_duration_days as daily_deal_display_ads_amt,
        subscription_revenue_amt/event_duration_days as daily_deal_subscription_revenue_amt,
        total_customers/event_duration_days as daily_deal_total_customers,
        new_customers/event_duration_days as daily_deal_new_customers,
        return_customers/event_duration_days as daily_deal_return_customers
    FROM base_metrics bm
        LEFT JOIN andes.BOOKER.D_MP_ASIN_MANUFACTURER mam
        ON mam.asin = bm.asin
        AND mam.marketplace_id = 7
        AND mam.region_id = 1
);


/*************************
Pre-deal metrics calculation
Include event context
*************************/
DROP TABLE IF EXISTS pre_deal_metrics;
CREATE TEMP TABLE pre_deal_metrics AS (
    SELECT 
        asin,
        item_name,
        event_name,
        promo_start_date,
        -- order_date,
        SUM(shipped_units)/91 as daily_pre_deal_shipped_units,  
        SUM(revenue_share_amt)/91 as daily_pre_deal_revenue,
        SUM(display_ads_amt)/91 as daily_pre_deal_display_ads_amt,
        SUM(subscription_revenue_amt)/91 as daily_pre_deal_subscription_revenue_amt,
        COUNT(DISTINCT customer_id)/91 as daily_pre_deal_total_customers,  
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers, 
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/91 as daily_pre_deal_return_customers
    FROM pre_deal_daily_summary
    GROUP BY 1,2,3,4
);


-- + booker.D_ASINS_MARKETPLACE_ATTRIBUTES.product_type (PL)
/*************************
Final table creation
*************************/
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_asin;
CREATE TABLE pm_sandbox_aqxiao.ntb_asin AS (
    SELECT 
        d.asin,
        d.item_name,
        CASE 
            WHEN d.gl_product_group = 510 THEN 'Lux Beauty'
            WHEN d.gl_product_group = 364 THEN 'Personal Care Appliances'    
            WHEN d.gl_product_group = 325 THEN 'Grocery'
            WHEN d.gl_product_group = 199 THEN 'Pet'
            WHEN d.gl_product_group = 194 THEN 'Beauty'
            WHEN d.gl_product_group = 121 THEN 'HPC'
            WHEN d.gl_product_group = 75 THEN 'Baby'    
        END as gl_product_group_name,
        d.vendor_code,
        v.company_name,
        v.company_code, 
        d.brand_code,
        d.brand_name,
        d.event_name,
        d.promo_start_date,
        d.promo_end_date,
        d.event_month,
        d.event_year,
        d.event_duration_days,
        
        -- Daily averages for comparison
        d.daily_deal_shipped_units as daily_deal_shipped_units,
        d.daily_deal_ops as daily_deal_ops,    
        d.daily_deal_display_ads_amt,
        d.daily_deal_subscription_revenue_amt,
        d.daily_deal_total_customers as daily_deal_total_customers,
        d.daily_deal_new_customers as daily_deal_new_customers,
        d.daily_deal_return_customers as daily_deal_return_customers,
        
        p.daily_pre_deal_shipped_units as daily_pre_deal_shipped_units,
        p.daily_pre_deal_revenue as daily_pre_deal_revenue,
        p.daily_pre_deal_display_ads_amt as daily_pre_deal_display_ads_amt,
        p.daily_pre_deal_subscription_revenue_amt as daily_pre_deal_subscription_revenue_amt,
        p.daily_pre_deal_total_customers as daily_pre_deal_total_customers,
        p.daily_pre_deal_new_customers as daily_pre_deal_new_customers,
        p.daily_pre_deal_return_customers as daily_pre_deal_return_customers,

        -- Growth calculations (comparing daily averages)
        ROUND(
            CASE 
                WHEN COALESCE(p.daily_pre_deal_new_customers, 0) = 0 
                THEN  ((d.daily_deal_new_customers::FLOAT / 0.000000001) - 1)
                ELSE ((d.daily_deal_new_customers::FLOAT / p.daily_pre_deal_new_customers) - 1) 
            END, 
            2
        ) as daily_new_customer_growth_pct,
        
        ROUND(
            CASE 
                WHEN COALESCE(p.daily_pre_deal_return_customers, 0) = 0 
                THEN ((d.daily_deal_return_customers::FLOAT / 0.000000001) - 1)
                ELSE ((d.daily_deal_return_customers::FLOAT / p.daily_pre_deal_return_customers) - 1)
            END,
            2
        ) as daily_return_customer_growth_pct
    FROM deal_metrics d
        LEFT JOIN pre_deal_metrics p
            ON d.asin = p.asin
            AND d.event_name = p.event_name
            AND d.promo_start_date = p.promo_start_date
        LEFT JOIN andes.roi_ml_ddl.VENDOR_COMPANY_CODES v
            ON v.vendor_code = d.vendor_code
    WHERE d.promo_start_date IS NOT NULL
    ORDER BY 
        d.promo_start_date DESC,
        d.daily_deal_ops * d.event_duration_days DESC
);



-- Grant permissions
-- GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_asin TO PUBLIC;
