/*************************
ntoes on SnS Metrics:
-- for sns metrics beyond ASIN level
-- use count(distinct customer_id) 
    FROM SUBS_SAVE_REPORTING.FCT_SNS_SALES_DETAILS_DAILY 
-- once subscription request approved
*************************/


/*************************
Base Orders Query
- Gets order data for consumables categories
- Includes only retail merchant orders with shipped units > 0 
- Excludes cancelled or fraudulent orders
- Filters for last 730 days 
*************************/
DROP TABLE IF EXISTS base_orders1;
CREATE TEMP TABLE base_orders1 AS (
    
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
        AND o.order_datetime BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '730 days' -- 2yr
            AND TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
);

-- + vendor code 
-- + gl name mapping
DROP TABLE IF EXISTS base_orders;
CREATE TEMP TABLE base_orders AS (
    SELECT 
        o.*,
        mam.dama_mfg_vendor_code as vendor_code,
        v.company_code,
        v.company_name,
        (CASE 
            WHEN o.gl_product_group = 510 THEN 'Lux Beauty'
            WHEN o.gl_product_group = 364 THEN 'Personal Care Appliances'    
            WHEN o.gl_product_group = 325 THEN 'Grocery'
            WHEN o.gl_product_group = 199 THEN 'Pet'
            WHEN o.gl_product_group = 194 THEN 'Beauty'
            WHEN o.gl_product_group = 121 THEN 'HPC'
            WHEN o.gl_product_group = 75 THEN 'Baby'    
        END) as gl_product_group_name
    FROM base_orders1 o
        LEFT JOIN andes.BOOKER.D_MP_ASIN_MANUFACTURER mam
            ON mam.asin = o.asin
            AND mam.marketplace_id = 7
            AND mam.region_id = 1
        LEFT JOIN andes.roi_ml_ddl.VENDOR_COMPANY_CODES v
            ON v.vendor_code = mam.dama_mfg_vendor_code

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
                OR UPPER(p.promotion_internal_title) LIKE '%PD%' 
                OR UPPER(p.promotion_internal_title) LIKE '%PEBD%' THEN 'PRIME DAY'
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
        JOIN 
            (
                SELECT 
                    promotion_key,
                    start_datetime,
                    end_datetime,
                    promotion_internal_title
                FROM andes.pdm.dim_promotion
                WHERE marketplace_key = 7
                    AND approval_status IN ('Approved', 'Scheduled')
                    AND promotion_type IN ('Best Deal', 'Deal of the Day', 'Lightning Deal', 'Event Deal')
                    AND UPPER(promotion_internal_title) NOT LIKE '%OIH%'
                    AND TO_DATE(start_datetime, 'YYYY-MM-DD') 
                        BETWEEN current_date - interval '730 days'
                        AND current_date
        ) p
        ON f.promotion_key = p.promotion_key
);


/*************************
Create a consolidated promotions table to handle overlapping periods
*************************/
DROP TABLE IF EXISTS consolidated_promos;
CREATE TEMP TABLE consolidated_promos AS (
    WITH overlapping_periods AS (
        SELECT 
            asin,
            customer_shipment_item_id,  
            event_name,
            DATE_PART('year', promo_start_date) as event_year, 
            DATE_PART('month', promo_start_date) as event_month,
            MIN(promo_start_date) as start_date,
            MAX(promo_end_date) as end_date
        FROM promotion_details
        GROUP BY 
            asin,
            customer_shipment_item_id,  
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
SNS Metrics - daily aggregations
*************************/
DROP TABLE IF EXISTS sns_metrics;
CREATE TEMP TABLE sns_metrics AS (
    SELECT 
        sns.asin,
        p.event_name,
        p.event_year,
        AVG(CASE 
            WHEN TO_DATE(snapshot_date, 'YYYY-MM-DD') BETWEEN p.promo_start_date AND p.promo_end_date 
            THEN active_subscription_count 
        END) as avg_deal_sns_subscribers,
        AVG(CASE 
            WHEN TO_DATE(snapshot_date, 'YYYY-MM-DD') BETWEEN p.promo_start_date - interval '91 day' AND p.promo_start_date - interval '1 day'
            THEN active_subscription_count 
        END) as avg_pre_deal_sns_subscribers
    FROM andes.subs_save_ddl.d_daily_active_sns_asin_detail sns
        INNER JOIN consolidated_promos p
        ON sns.asin = p.asin
    WHERE sns.marketplace_id = 7
        AND sns.gl_product_group in (510, 364, 325, 199, 194, 121, 75)
    GROUP BY 
        sns.asin,
        p.event_name,
        p.event_year
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
            WHEN p.promo_end_date >= current_date
                THEN current_date - p.promo_start_date + 1
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
    SELECT DISTINCT
        asin,
        MIN(promo_start_date) as promo_start_date,
        MAX(promo_end_date) as promo_end_date,
        event_name,
        event_year,
        event_month,
        MIN(promo_start_date) - interval '91 day' AS pre_deal_start_date,
        MIN(promo_start_date) - interval '1 day' AS pre_deal_end_date
    FROM consolidated_promos
    GROUP BY 
        asin,
        event_name,
        event_year,
        event_month
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
    WHERE b.order_date 
        BETWEEN pdr.pre_deal_start_date 
        AND pdr.pre_deal_end_date
);


/*************************
First purchase PER CUSTOMER to brand 
*************************/
DROP TABLE IF EXISTS first_purchases;
CREATE TEMP TABLE first_purchases AS (
    SELECT 
        customer_id,
        deal_orders.brand_code, 
        MIN(order_date) as first_purchase_date
    FROM (
        SELECT customer_id, brand_code, order_date FROM deal_orders
        UNION ALL
        SELECT customer_id, brand_code, order_date FROM pre_deal_orders
    ) deal_orders  
    WHERE deal_orders.brand_code IS NOT NULL  
    GROUP BY 
        customer_id,
        deal_orders.brand_code
);


/*************************
PER CUSTOMER PER DAY summary
Split by period type but using same first_purchases
*************************/
DROP TABLE IF EXISTS deal_daily_summary;
CREATE TEMP TABLE deal_daily_summary AS (
    SELECT 
        o.asin,
        o.item_name,
        o.gl_product_group,
        o.gl_product_group_name,
        o.vendor_code,
        o.company_code,
        o.company_name,
        o.brand_code,
        o.brand_name,
        o.event_name,
        o.promo_start_date,
        o.promo_end_date,
        o.event_duration_days,
        o.event_year,
        o.event_month, 
        o.period_type,
        o.order_date,
        o.customer_id,
        (CASE 
            WHEN o.order_date = fp.first_purchase_date THEN 1 
            ELSE 0 
        END) AS is_first_brand_purchase,
        o.shipped_units,
        o.revenue_share_amt,
        s.avg_pre_deal_sns_subscribers as daily_deal_sns_subscribers

    FROM deal_orders o
        LEFT JOIN first_purchases fp
            ON o.customer_id = fp.customer_id
            AND o.brand_code = fp.brand_code
        LEFT JOIN sns_metrics s
            ON o.asin = s.asin
            AND o.event_name = s.event_name
            AND o.event_year = s.event_year
);


/*************************
Pre-deal daily summary
*************************/
DROP TABLE IF EXISTS pre_deal_daily_summary;
CREATE TEMP TABLE pre_deal_daily_summary AS (
    SELECT 
        o.asin,
        o.item_name,
        o.gl_product_group,
        o.gl_product_group_name,
        o.vendor_code,
        o.company_code,
        o.company_name,
        o.brand_code,
        o.brand_name,
        o.event_name,
        o.promo_start_date,
        o.promo_end_date,
        91 as event_duration_days,
        o.event_year,
        o.event_month, 
        o.period_type,
        o.order_date,
        o.customer_id,
        (CASE 
            WHEN o.order_date = fp.first_purchase_date THEN 1 
            ELSE 0 
        END) AS is_first_brand_purchase,
        o.shipped_units,
        o.revenue_share_amt,
        s.avg_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers

    FROM pre_deal_orders o
        LEFT JOIN first_purchases fp
            ON o.customer_id = fp.customer_id
            AND o.brand_code = fp.brand_code
        LEFT JOIN sns_metrics s
            ON o.asin = s.asin
            AND o.event_name = s.event_name
            AND o.event_year = s.event_year
);


/*************************
Deal metrics calculation (daily avg)
*************************/
DROP TABLE IF EXISTS deal_metrics;
CREATE TEMP TABLE deal_metrics AS (
    
    with base_metrics as (
        SELECT 
            asin,
            item_name,
            gl_product_group,
            gl_product_group_name,
            brand_code,
            brand_name,
            vendor_code,
            company_code, 
            company_name,
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
            daily_deal_sns_subscribers,

            -- ASIN level metrics
            shipped_units,
            revenue_share_amt,
            customer_id,
            is_first_brand_purchase
        
        FROM deal_daily_summary
    ),

    base_agg as (
        SELECT
            asin,
            item_name,
            gl_product_group,
            gl_product_group_name,
            brand_code,
            brand_name,
            vendor_code,
            company_code, 
            company_name,
            event_name,
            promo_start_date,
            promo_end_date,
            event_month,
            event_year,
            event_duration_days,
            daily_deal_sns_subscribers,
            SUM(shipped_units) as total_shipped_units,
            SUM(revenue_share_amt) as revenue,
            COUNT(DISTINCT customer_id) as total_customers_asin,
            COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END) as new_customers_asin,
            COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END) as return_customers_asin
        FROM base_metrics
        GROUP BY 
            asin,
            item_name,
            gl_product_group,
            gl_product_group_name,
            brand_code,
            brand_name,
            vendor_code,
            company_code, 
            company_name,
            event_name,
            promo_start_date,
            promo_end_date,
            event_month,
            event_year,
            event_duration_days,
            daily_deal_sns_subscribers
    )

    SELECT 
        asin,
        item_name,
        gl_product_group,
        gl_product_group_name,
        brand_code,
        brand_name,
        vendor_code,
        company_code, 
        company_name,
        event_name,
        promo_start_date,
        promo_end_date,
        event_month,
        event_year,
        event_duration_days,
        daily_deal_sns_subscribers,

        -- ASIN level daily metrics
        total_shipped_units/event_duration_days as daily_deal_shipped_units,
        revenue/event_duration_days as daily_deal_ops,            
        total_customers_asin/event_duration_days as daily_deal_total_customers_asin,
        new_customers_asin/event_duration_days as daily_deal_new_customers_asin,
        return_customers_asin/event_duration_days as daily_deal_return_customers_asin 

    FROM base_agg bm
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
        gl_product_group,
        gl_product_group_name,
        brand_code,
        brand_name,
        vendor_code,
        company_code, 
        company_name,
        event_name,
        promo_start_date,
        promo_end_date,
        event_month,
        event_year,
        event_duration_days,
        daily_pre_deal_sns_subscribers,
        SUM(shipped_units)/91 as daily_pre_deal_shipped_units,  
        SUM(revenue_share_amt)/91 as daily_pre_deal_revenue,
        COUNT(DISTINCT customer_id)/91 as daily_pre_deal_total_customers_asin,  
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers_asin
    FROM pre_deal_daily_summary
    GROUP BY
        asin,
        item_name,
        gl_product_group,
        gl_product_group_name,
        brand_code,
        brand_name,
        vendor_code,
        company_code, 
        company_name,
        event_name,
        promo_start_date,
        promo_end_date,
        event_month,
        event_year,
        event_duration_days,
        daily_pre_deal_sns_subscribers
);



-- + booker.D_ASINS_MARKETPLACE_ATTRIBUTES.product_type (PL)
/*************************
Compare deal vs pre deal periods (delta)
*************************/
DROP TABLE IF EXISTS deal_growth;
CREATE TEMP TABLE deal_growth AS (
    SELECT 
        -- asin info
        d.asin,
        d.item_name,
        d.gl_product_group_name,
        d.vendor_code,
        d.company_name,
        d.company_code, 
        d.brand_code,
        d.brand_name,
        -- event info
        d.event_name,
        d.promo_start_date,
        d.promo_end_date,
        d.event_month,
        d.event_year,
        d.event_duration_days,
        
        -- during deal daily avg (ASIN level)
        d.daily_deal_shipped_units,
        d.daily_deal_ops,    
        -- d.daily_deal_subscription_revenue_amt,
        d.daily_deal_total_customers_asin,
        d.daily_deal_new_customers_asin,
        -- sns
        s.avg_deal_sns_subscribers as daily_deal_sns_subscribers,

        -- pre deal daily avg (ASIN level)
        p.daily_pre_deal_shipped_units,
        p.daily_pre_deal_revenue,
        -- p.daily_pre_deal_subscription_revenue_amt,
        p.daily_pre_deal_total_customers_asin,
        p.daily_pre_deal_new_customers_asin,
        -- sns
        s.avg_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers,

        -- Calculate Delta Metrics
        (d.daily_deal_shipped_units - p.daily_pre_deal_shipped_units) as delta_daily_shipped_units_asin,
        (d.daily_deal_ops - p.daily_pre_deal_revenue) as delta_daily_revenue_asin,
        (d.daily_deal_total_customers_asin - p.daily_pre_deal_total_customers_asin) as delta_daily_customers_asin,
        (d.daily_deal_new_customers_asin - p.daily_pre_deal_new_customers_asin) as delta_daily_new_customers_asin,
        (s.avg_deal_sns_subscribers - s.avg_pre_deal_sns_subscribers) as delta_daily_sns_subscribers_asin

    FROM deal_metrics d
        LEFT JOIN pre_deal_metrics p
            ON d.asin = p.asin
            AND d.event_name = p.event_name
            AND d.event_year = p.event_year 
        LEFT JOIN sns_metrics s
            ON d.asin = s.asin
            AND d.event_name = s.event_name
            AND d.event_year = s.event_year
    
    WHERE d.promo_start_date IS NOT NULL
    ORDER BY 
        d.promo_start_date DESC,
        d.daily_deal_ops * d.event_duration_days DESC
);


/*************************
Calculate Last Year metrics
*************************/
DROP TABLE IF EXISTS final_asin_metrics;
CREATE TEMP TABLE final_asin_metrics AS (
    SELECT
        t1.asin,
        t1.item_name,
        t1.gl_product_group_name,
        t1.vendor_code,
        t1.company_name,
        t1.company_code,
        t1.brand_code,
        t1.brand_name,
        t1.event_name,
        t1.event_year,
        t1.event_duration_days,

        -- Current Year Deal Period Metrics
        t1.daily_deal_shipped_units as daily_deal_shipped_units_asin,
        t1.daily_deal_ops as daily_deal_revenue_asin,
        t1.daily_deal_total_customers_asin as daily_deal_customers_asin,
        t1.daily_deal_new_customers_asin,
        t1.daily_deal_sns_subscribers as daily_deal_sns_subscribers_asin,

        -- Current Year Pre-Deal Period Metrics
        t1.daily_pre_deal_shipped_units as daily_pre_deal_shipped_units_asin,
        t1.daily_pre_deal_revenue as daily_pre_deal_revenue_asin,
        t1.daily_pre_deal_total_customers_asin as daily_pre_deal_customers_asin,
        t1.daily_pre_deal_new_customers_asin,
        t1.daily_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers_asin,

        -- Last Year Deal Period Metrics
        t2.daily_deal_shipped_units as ly_daily_deal_shipped_units_asin,
        t2.daily_deal_ops as ly_daily_deal_revenue_asin,
        t2.daily_deal_total_customers_asin as ly_daily_deal_customers_asin,
        t2.daily_deal_new_customers_asin as ly_daily_deal_new_customers_asin,
        t2.daily_deal_sns_subscribers as ly_daily_deal_sns_subscribers_asin,

        -- Last Year Pre-Deal Period Metrics
        t2.daily_pre_deal_shipped_units as ly_daily_pre_deal_shipped_units_asin,
        t2.daily_pre_deal_revenue as ly_daily_pre_deal_revenue_asin,
        t2.daily_pre_deal_total_customers_asin as ly_daily_pre_deal_customers_asin,
        t2.daily_pre_deal_new_customers_asin as ly_daily_pre_deal_new_customers_asin,
        t2.daily_pre_deal_sns_subscribers as ly_daily_pre_deal_sns_subscribers_asin,

        -- Delta Metrics
        t1.delta_daily_shipped_units_asin,
        t1.delta_daily_revenue_asin,
        t1.delta_daily_customers_asin,
        t1.delta_daily_new_customers_asin,
        t1.delta_daily_sns_subscribers_asin

    FROM deal_growth t1
        LEFT JOIN deal_growth t2
        ON t1.asin = t2.asin
        AND t1.event_name = t2.event_name
        AND t1.event_year - 1 = t2.event_year
    WHERE t1.event_name IS NOT NULL
);

/*************************
# 1. FINAL TABLE CREATION
-- ASIN Level 
*************************/
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_asin_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_asin_level AS (
    SELECT 
        -- Base ASIN info
        asin,
        item_name,
        gl_product_group_name,
        vendor_code,
        company_name,
        company_code,
        brand_code,
        brand_name,
        event_name,
        event_year,
        event_duration_days,
        
        -- Deal Period Metrics
        daily_deal_shipped_units_asin,
        daily_deal_revenue_asin,
        daily_deal_customers_asin,
        daily_deal_new_customers_asin,
        daily_deal_sns_subscribers_asin,
        
        -- Pre-Deal Period Metrics
        daily_pre_deal_shipped_units_asin,
        daily_pre_deal_revenue_asin,
        daily_pre_deal_customers_asin,
        daily_pre_deal_new_customers_asin,
        daily_pre_deal_sns_subscribers_asin,
        
        -- Delta Metrics
        delta_daily_shipped_units_asin,
        delta_daily_revenue_asin,
        delta_daily_customers_asin,
        delta_daily_new_customers_asin,
        delta_daily_sns_subscribers_asin,
        
        -- Last Year Metrics
        ly_daily_deal_shipped_units_asin,
        ly_daily_deal_revenue_asin,
        ly_daily_deal_customers_asin,
        ly_daily_deal_new_customers_asin,
        ly_daily_deal_sns_subscribers_asin,
        ly_daily_pre_deal_shipped_units_asin,
        ly_daily_pre_deal_revenue_asin,
        ly_daily_pre_deal_customers_asin,
        ly_daily_pre_deal_new_customers_asin,
        ly_daily_pre_deal_sns_subscribers_asin
    FROM final_asin_metrics
);




/*************************
Prep for final output
Brand Level intermediary tables
*************************/
-- 1. Create base metrics combining deal and pre-deal in one pass
DROP TABLE IF EXISTS brand_base_metrics;
CREATE TEMP TABLE brand_base_metrics AS (
    SELECT 
        gl_product_group,
        gl_product_group_name,
        vendor_code,
        company_code,
        company_name,
        brand_code,
        brand_name,
        event_name,
        event_year,
        MAX(CASE WHEN period_type = 'DEAL' THEN event_duration_days END) as event_duration_days,
        -- Deal metrics
        SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END)/NULLIF(MAX(CASE WHEN period_type = 'DEAL' THEN event_duration_days END),0) as daily_deal_shipped_units_brand,
        SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END)/NULLIF(MAX(CASE WHEN period_type = 'DEAL' THEN event_duration_days END),0) as daily_deal_revenue_brand,
        COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END)/NULLIF(MAX(CASE WHEN period_type = 'DEAL' THEN event_duration_days END),0) as daily_deal_customers_brand,
        COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/NULLIF(MAX(CASE WHEN period_type = 'DEAL' THEN event_duration_days END),0) as daily_deal_new_customers_brand,
        COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/NULLIF(MAX(CASE WHEN period_type = 'DEAL' THEN event_duration_days END),0) as daily_deal_return_customers_brand,
        -- Pre-deal metrics
        SUM(CASE WHEN period_type = 'PRE_DEAL' THEN shipped_units END)/91 as daily_pre_deal_shipped_units_brand,
        SUM(CASE WHEN period_type = 'PRE_DEAL' THEN revenue_share_amt END)/91 as daily_pre_deal_revenue_brand,
        COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END)/91 as daily_pre_deal_customers_brand,
        COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers_brand,
        COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/91 as daily_pre_deal_return_customers_brand
    FROM (
        SELECT * FROM deal_daily_summary WHERE period_type = 'DEAL'
        UNION ALL 
        SELECT * FROM pre_deal_daily_summary WHERE period_type = 'PRE_DEAL'
    )
    GROUP BY 1,2,3,4,5,6,7,8,9
);

-- 2. Add SNS metrics
DROP TABLE IF EXISTS brand_metrics_with_sns;
CREATE TEMP TABLE brand_metrics_with_sns AS (
    SELECT 
        b.*,
        s.avg_deal_sns_subscribers_brand as daily_deal_sns_subscribers_brand,
        s.avg_pre_deal_sns_subscribers_brand as daily_pre_deal_sns_subscribers_brand,
        s.avg_deal_sns_subscribers_brand - s.avg_pre_deal_sns_subscribers_brand as delta_daily_sns_subscribers_brand
    FROM brand_base_metrics b
    LEFT JOIN brand_sns_sums s
        ON b.brand_code = s.brand_code
        AND b.event_name = s.event_name
        AND b.event_year = s.event_year
);

-- 3. Add delta calculations
DROP TABLE IF EXISTS brand_metrics_with_deltas;
CREATE TEMP TABLE brand_metrics_with_deltas AS (
    SELECT 
        *,
        daily_deal_shipped_units_brand - daily_pre_deal_shipped_units_brand as delta_daily_shipped_units_brand,
        daily_deal_revenue_brand - daily_pre_deal_revenue_brand as delta_daily_revenue_brand,
        daily_deal_customers_brand - daily_pre_deal_customers_brand as delta_daily_customers_brand,
        daily_deal_new_customers_brand - daily_pre_deal_new_customers_brand as delta_daily_new_customers_brand,
        daily_deal_return_customers_brand - daily_pre_deal_return_customers_brand as delta_daily_return_customers_brand
    FROM brand_metrics_with_sns
);

-- 4. Add last year metrics
DROP TABLE IF EXISTS final_brand_metrics;
CREATE TEMP TABLE final_brand_metrics AS (
    SELECT 
        curr.*,
        prev.daily_deal_shipped_units_brand as ly_daily_deal_shipped_units_brand,
        prev.daily_deal_revenue_brand as ly_daily_deal_revenue_brand,
        prev.daily_deal_customers_brand as ly_daily_deal_customers_brand,
        prev.daily_deal_new_customers_brand as ly_daily_deal_new_customers_brand,
        prev.daily_deal_return_customers_brand as ly_daily_deal_return_customers_brand,
        prev.daily_pre_deal_shipped_units_brand as ly_daily_pre_deal_shipped_units_brand,
        prev.daily_pre_deal_revenue_brand as ly_daily_pre_deal_revenue_brand,
        prev.daily_pre_deal_customers_brand as ly_daily_pre_deal_customers_brand,
        prev.daily_pre_deal_new_customers_brand as ly_daily_pre_deal_new_customers_brand,
        prev.daily_pre_deal_return_customers_brand as ly_daily_pre_deal_return_customers_brand,
        prev.daily_deal_sns_subscribers_brand as ly_daily_deal_sns_subscribers_brand,
        prev.daily_pre_deal_sns_subscribers_brand as ly_daily_pre_deal_sns_subscribers_brand
    FROM brand_metrics_with_deltas curr
        LEFT JOIN brand_metrics_with_deltas prev
        ON curr.brand_code = prev.brand_code
        AND curr.event_name = prev.event_name
        AND curr.event_year = prev.event_year + 1
);



/*************************
# 2. FINAL TABLE CREATION
-- Brand Level 
*************************/
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_brand_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_brand_level AS (
    SELECT 
        -- Base Brand info
        gl_product_group,
        gl_product_group_name,
        vendor_code,
        company_code,
        company_name,
        brand_code,
        brand_name,
        -- Event info
        event_name,
        event_year,
        event_duration_days,
        
        -- Deal Period Metrics
        daily_deal_shipped_units_brand,
        daily_deal_revenue_brand,
        daily_deal_customers_brand,
        daily_deal_new_customers_brand,
        daily_deal_return_customers_brand,
        daily_deal_sns_subscribers_brand,
        
        -- Pre-Deal Period Metrics
        daily_pre_deal_shipped_units_brand,
        daily_pre_deal_revenue_brand,
        daily_pre_deal_customers_brand,
        daily_pre_deal_new_customers_brand,
        daily_pre_deal_return_customers_brand,
        daily_pre_deal_sns_subscribers_brand,
        
        -- Delta Metrics
        delta_daily_shipped_units_brand,
        delta_daily_revenue_brand,
        delta_daily_customers_brand,
        delta_daily_new_customers_brand,
        delta_daily_return_customers_brand,
        delta_daily_sns_subscribers_brand,
        
        -- Last Year Metrics
        ly_daily_deal_shipped_units_brand,
        ly_daily_deal_revenue_brand,
        ly_daily_deal_customers_brand,
        ly_daily_deal_new_customers_brand,
        ly_daily_deal_return_customers_brand,
        ly_daily_pre_deal_shipped_units_brand,
        ly_daily_pre_deal_revenue_brand,
        ly_daily_pre_deal_customers_brand,
        ly_daily_pre_deal_new_customers_brand,
        ly_daily_pre_deal_return_customers_brand,
        ly_daily_deal_sns_subscribers_brand,
        ly_daily_pre_deal_sns_subscribers_brand
    FROM final_brand_metrics
);


/*************************
Prep for final output
Company Level intermediary tables
*************************/
-- 1.1 Create base company metrics for deal period
DROP TABLE IF EXISTS company_deal_base;
CREATE TEMP TABLE company_deal_base AS (
    SELECT 
        gl_product_group,
        gl_product_group_name,
        company_code,
        company_name,
        event_name,
        event_year,
        event_duration_days,
        -- calculate daily averages for deal period
        SUM(shipped_units)/MAX(event_duration_days) as daily_deal_shipped_units_company,
        SUM(revenue_share_amt)/MAX(event_duration_days) as daily_deal_revenue_company,
        COUNT(DISTINCT customer_id)/MAX(event_duration_days) as daily_deal_customers_company,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/MAX(event_duration_days) as daily_deal_new_customers_company,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/MAX(event_duration_days) as daily_deal_return_customers_company
    FROM deal_daily_summary
    WHERE period_type = 'DEAL'
    GROUP BY 1,2,3,4,5,6,7
);

-- 1.2 Create base company metrics for pre-deal period
DROP TABLE IF EXISTS company_pre_deal_base;
CREATE TEMP TABLE company_pre_deal_base AS (
    SELECT 
        company_code,
        event_name,
        event_year,
        -- Properly calculate daily averages for pre-deal period (always 91 days)
        SUM(shipped_units)/91 as daily_pre_deal_shipped_units_company,
        SUM(revenue_share_amt)/91 as daily_pre_deal_revenue_company,
        COUNT(DISTINCT customer_id)/91 as daily_pre_deal_customers_company,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers_company,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/91 as daily_pre_deal_return_customers_company
    FROM pre_deal_daily_summary
    WHERE period_type = 'PRE_DEAL'
    GROUP BY 1,2,3
);

-- 1.3 Combine company metrics with SNS data
DROP TABLE IF EXISTS company_combined_metrics;
CREATE TEMP TABLE company_combined_metrics AS (
    SELECT 
        d.*,
        p.daily_pre_deal_shipped_units_company,
        p.daily_pre_deal_revenue_company,
        p.daily_pre_deal_customers_company,
        p.daily_pre_deal_new_customers_company,
        p.daily_pre_deal_return_customers_company,
        -- Add SNS metrics
        s.avg_deal_sns_subscribers_company as daily_deal_sns_subscribers_company,
        s.avg_pre_deal_sns_subscribers_company as daily_pre_deal_sns_subscribers_company
    FROM company_deal_base d
        LEFT JOIN company_pre_deal_base p
            ON d.company_code = p.company_code
            AND d.event_name = p.event_name
            AND d.event_year = p.event_year
        LEFT JOIN company_sns_sums s
            ON d.company_code = s.company_code
            AND d.event_name = s.event_name
            AND d.event_year = s.event_year
);

-- 1.4 Add delta calculations
DROP TABLE IF EXISTS company_metrics_with_deltas;
CREATE TEMP TABLE company_metrics_with_deltas AS (
    SELECT 
        *,
        daily_deal_shipped_units_company - daily_pre_deal_shipped_units_company as delta_daily_shipped_units_company,
        daily_deal_revenue_company - daily_pre_deal_revenue_company as delta_daily_revenue_company,
        daily_deal_customers_company - daily_pre_deal_customers_company as delta_daily_customers_company,
        daily_deal_new_customers_company - daily_pre_deal_new_customers_company as delta_daily_new_customers_company,
        daily_deal_return_customers_company - daily_pre_deal_return_customers_company as delta_daily_return_customers_company,
        daily_deal_sns_subscribers_company - daily_pre_deal_sns_subscribers_company as delta_daily_sns_subscribers_company
    FROM company_combined_metrics
);

-- 2.1 First partition current and previous year data
DROP TABLE IF EXISTS company_metrics_by_year;
CREATE TEMP TABLE company_metrics_by_year AS (
    SELECT 
        *,
        CASE 
            WHEN event_year = (SELECT MAX(event_year) FROM company_metrics_with_deltas)
                THEN 'CURRENT'
            WHEN event_year = (SELECT MAX(event_year) - 1 FROM company_metrics_with_deltas)
                THEN 'PREVIOUS'
            ELSE 'OTHER'
        END as year_type
    FROM company_metrics_with_deltas
);

-- 2.2 Then join only the relevant partitions
DROP TABLE IF EXISTS final_company_metrics;
CREATE TEMP TABLE final_company_metrics AS (
    SELECT 
        curr.*,
        -- Last year metrics
        prev.daily_deal_shipped_units_company as ly_daily_deal_shipped_units_company,
        prev.daily_deal_revenue_company as ly_daily_deal_revenue_company,
        prev.daily_deal_customers_company as ly_daily_deal_customers_company,
        prev.daily_deal_new_customers_company as ly_daily_deal_new_customers_company,
        prev.daily_deal_return_customers_company as ly_daily_deal_return_customers_company,
        prev.daily_pre_deal_shipped_units_company as ly_daily_pre_deal_shipped_units_company,
        prev.daily_pre_deal_revenue_company as ly_daily_pre_deal_revenue_company,
        prev.daily_pre_deal_customers_company as ly_daily_pre_deal_customers_company,
        prev.daily_pre_deal_new_customers_company as ly_daily_pre_deal_new_customers_company,
        prev.daily_pre_deal_return_customers_company as ly_daily_pre_deal_return_customers_company,
        prev.daily_deal_sns_subscribers_company as ly_daily_deal_sns_subscribers_company,
        prev.daily_pre_deal_sns_subscribers_company as ly_daily_pre_deal_sns_subscribers_company
    FROM 
        (SELECT * FROM company_metrics_by_year WHERE year_type = 'CURRENT') curr
        LEFT JOIN 
        (SELECT * FROM company_metrics_by_year WHERE year_type = 'PREVIOUS') prev
            ON curr.company_code = prev.company_code
            AND curr.event_name = prev.event_name
);

-- Final output company level
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_company_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_company_level AS (
    SELECT 
        -- Base Company info
        gl_product_group,
        gl_product_group_name,
        company_code,
        company_name,
        -- Event info
        event_name,
        event_year,
        event_duration_days,
        
        -- Deal Period Metrics
        daily_deal_shipped_units_company,
        daily_deal_revenue_company,
        daily_deal_customers_company,
        daily_deal_new_customers_company,
        daily_deal_return_customers_company,
        daily_deal_sns_subscribers_company,
        
        -- Pre-Deal Period Metrics
        daily_pre_deal_shipped_units_company,
        daily_pre_deal_revenue_company,
        daily_pre_deal_customers_company,
        daily_pre_deal_new_customers_company,
        daily_pre_deal_return_customers_company,
        daily_pre_deal_sns_subscribers_company,
        
        -- Delta Metrics
        delta_daily_shipped_units_company,
        delta_daily_revenue_company,
        delta_daily_customers_company,
        delta_daily_new_customers_company,
        delta_daily_return_customers_company,
        delta_daily_sns_subscribers_company,
        
        -- Last Year Metrics
        ly_daily_deal_shipped_units_company,
        ly_daily_deal_revenue_company,
        ly_daily_deal_customers_company,
        ly_daily_deal_new_customers_company,
        ly_daily_deal_return_customers_company,
        ly_daily_pre_deal_shipped_units_company,
        ly_daily_pre_deal_revenue_company,
        ly_daily_pre_deal_customers_company,
        ly_daily_pre_deal_new_customers_company,
        ly_daily_pre_deal_return_customers_company,
        ly_daily_deal_sns_subscribers_company,
        ly_daily_pre_deal_sns_subscribers_company
    FROM final_company_metrics
);


/*************************
#4. Final table creation
-- GL Level 
*************************/

-- 1. First create base GL metrics for deal period
DROP TABLE IF EXISTS gl_deal_base;
CREATE TEMP TABLE gl_deal_base AS (
    SELECT 
        gl_product_group,
        gl_product_group_name,
        event_name,
        event_year,
        event_duration_days,
        -- Properly calculate daily averages for deal period
        SUM(shipped_units)/MAX(event_duration_days) as daily_deal_shipped_units_gl,
        SUM(revenue_share_amt)/MAX(event_duration_days) as daily_deal_revenue_gl,
        COUNT(DISTINCT customer_id)/MAX(event_duration_days) as daily_deal_customers_gl,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/MAX(event_duration_days) as daily_deal_new_customers_gl,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/MAX(event_duration_days) as daily_deal_return_customers_gl

    FROM deal_daily_summary
    WHERE period_type = 'DEAL'
    GROUP BY 1,2,3,4,5
);

-- 2. Create base GL metrics for pre-deal period
DROP TABLE IF EXISTS gl_pre_deal_base;
CREATE TEMP TABLE gl_pre_deal_base AS (
    SELECT 
        gl_product_group,
        event_name,
        event_year,
        -- Properly calculate daily averages for pre-deal period (always 91 days)
        SUM(shipped_units)/91 as daily_pre_deal_shipped_units_gl,
        SUM(revenue_share_amt)/91 as daily_pre_deal_revenue_gl,
        COUNT(DISTINCT customer_id)/91 as daily_pre_deal_customers_gl,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers_gl,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/91 as daily_pre_deal_return_customers_gl

    FROM pre_deal_daily_summary
    WHERE period_type = 'PRE_DEAL'
    GROUP BY 1,2,3
);

-- 3. Combine GL metrics with SNS data
DROP TABLE IF EXISTS gl_combined_metrics;
CREATE TEMP TABLE gl_combined_metrics AS (
    SELECT 
        d.*,
        p.daily_pre_deal_shipped_units_gl,
        p.daily_pre_deal_revenue_gl,
        p.daily_pre_deal_customers_gl,
        p.daily_pre_deal_new_customers_gl,
        p.daily_pre_deal_return_customers_gl,
        -- Add SNS metrics
        s.avg_deal_sns_subscribers_gl as daily_deal_sns_subscribers_gl,
        s.avg_pre_deal_sns_subscribers_gl as daily_pre_deal_sns_subscribers_gl
    FROM gl_deal_base d
        LEFT JOIN gl_pre_deal_base p
            ON d.gl_product_group = p.gl_product_group
            AND d.event_name = p.event_name
            AND d.event_year = p.event_year
        LEFT JOIN gl_sns_sums s
            ON d.gl_product_group = s.gl_product_group
            AND d.event_name = s.event_name
            AND d.event_year = s.event_year
);

-- 4. Add delta calculations
DROP TABLE IF EXISTS gl_metrics_with_deltas;
CREATE TEMP TABLE gl_metrics_with_deltas AS (
    SELECT 
        *,
        daily_deal_shipped_units_gl - daily_pre_deal_shipped_units_gl as delta_daily_shipped_units_gl,
        daily_deal_revenue_gl - daily_pre_deal_revenue_gl as delta_daily_revenue_gl,
        daily_deal_customers_gl - daily_pre_deal_customers_gl as delta_daily_customers_gl,
        daily_deal_new_customers_gl - daily_pre_deal_new_customers_gl as delta_daily_new_customers_gl,
        daily_deal_return_customers_gl - daily_pre_deal_return_customers_gl as delta_daily_return_customers_gl,
        daily_deal_sns_subscribers_gl - daily_pre_deal_sns_subscribers_gl as delta_daily_sns_subscribers_gl

    FROM gl_combined_metrics
);

-- 5. Final GL metrics with last year comparison
DROP TABLE IF EXISTS final_gl_metrics;
CREATE TEMP TABLE final_gl_metrics AS (
    SELECT 
        curr.*,
        -- Last year metrics
        ly.daily_deal_shipped_units_gl as ly_daily_deal_shipped_units_gl,
        ly.daily_deal_revenue_gl as ly_daily_deal_revenue_gl,
        ly.daily_deal_customers_gl as ly_daily_deal_customers_gl,
        ly.daily_deal_new_customers_gl as ly_daily_deal_new_customers_gl,
        ly.daily_deal_return_customers_gl as ly_daily_deal_return_customers_gl,
        ly.deal_brand_count as ly_deal_brand_count,
        ly.deal_company_count as ly_deal_company_count,
        ly.daily_pre_deal_shipped_units_gl as ly_daily_pre_deal_shipped_units_gl,
        ly.daily_pre_deal_revenue_gl as ly_daily_pre_deal_revenue_gl,
        ly.daily_pre_deal_customers_gl as ly_daily_pre_deal_customers_gl,
        ly.daily_pre_deal_new_customers_gl as ly_daily_pre_deal_new_customers_gl,
        ly.daily_pre_deal_return_customers_gl as ly_daily_pre_deal_return_customers_gl,
        -- Last year SNS metrics
        ly.daily_deal_sns_subscribers_gl as ly_daily_deal_sns_subscribers_gl,
        ly.daily_pre_deal_sns_subscribers_gl as ly_daily_pre_deal_sns_subscribers_gl
    FROM gl_metrics_with_deltas curr
        LEFT JOIN gl_metrics_with_deltas ly
            ON curr.gl_product_group = ly.gl_product_group
            AND curr.event_name = ly.event_name
            AND curr.event_year = ly.event_year + 1
);

-- 6. Create final GL level table
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_gl_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_gl_level AS (
    SELECT 
        -- Base GL info
        gl_product_group,
        gl_product_group_name,
        -- Event info
        event_name,
        event_year,
        event_duration_days,
        
        -- Deal Period Metrics
        daily_deal_shipped_units_gl,
        daily_deal_revenue_gl,
        daily_deal_customers_gl,
        daily_deal_new_customers_gl,
        daily_deal_return_customers_gl,
        daily_deal_sns_subscribers_gl,
        deal_brand_count,
        deal_company_count,
        
        -- Pre-Deal Period Metrics
        daily_pre_deal_shipped_units_gl,
        daily_pre_deal_revenue_gl,
        daily_pre_deal_customers_gl,
        daily_pre_deal_new_customers_gl,
        daily_pre_deal_return_customers_gl,
        daily_pre_deal_sns_subscribers_gl,

        
        -- Delta Metrics
        delta_daily_shipped_units_gl,
        delta_daily_revenue_gl,
        delta_daily_customers_gl,
        delta_daily_new_customers_gl,
        delta_daily_return_customers_gl,
        delta_daily_sns_subscribers_gl,

        
        -- Last Year Metrics
        ly_daily_deal_shipped_units_gl,
        ly_daily_deal_revenue_gl,
        ly_daily_deal_customers_gl,
        ly_daily_deal_new_customers_gl,
        ly_daily_deal_return_customers_gl,
        ly_deal_brand_count,
        ly_deal_company_count,
        ly_daily_pre_deal_shipped_units_gl,
        ly_daily_pre_deal_revenue_gl,
        ly_daily_pre_deal_customers_gl,
        ly_daily_pre_deal_new_customers_gl,
        ly_daily_pre_deal_return_customers_gl,
        ly_daily_deal_sns_subscribers_gl,
        ly_daily_pre_deal_sns_subscribers_gl

    FROM final_gl_metrics
);



/*************************
# 5. Final table creation
-- Event Level 
*************************/

DROP TABLE IF EXISTS asin_deal_base;
CREATE TEMP TABLE asin_deal_base AS (
    SELECT 
        asin,
        item_name,
        gl_product_group,
        gl_product_group_name,
        vendor_code,
        company_code,
        company_name,
        brand_code,
        brand_name,
        event_name,
        event_year,
        MAX(event_duration_days) as event_duration_days,
        -- Calculate total and daily averages for deal period
        SUM(shipped_units) as total_deal_shipped_units,
        SUM(shipped_units)/MAX(event_duration_days) as daily_deal_shipped_units,
        SUM(revenue_share_amt) as total_deal_revenue,
        SUM(revenue_share_amt)/MAX(event_duration_days) as daily_deal_revenue,
        COUNT(DISTINCT customer_id) as total_deal_customers,
        COUNT(DISTINCT customer_id)/MAX(event_duration_days) as daily_deal_customers,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END) as total_deal_new_customers,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/MAX(event_duration_days) as daily_deal_new_customers,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END) as total_deal_return_customers,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/MAX(event_duration_days) as daily_deal_return_customers
    FROM deal_daily_summary
    WHERE period_type = 'DEAL'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
);

-- 2. Create base ASIN metrics for pre-deal period
DROP TABLE IF EXISTS asin_pre_deal_base;
CREATE TEMP TABLE asin_pre_deal_base AS (
    SELECT 
        asin,
        event_name,
        event_year,
        -- Calculate total and daily averages for pre-deal period (always 91 days)
        SUM(shipped_units) as total_pre_deal_shipped_units,
        SUM(shipped_units)/91 as daily_pre_deal_shipped_units,
        SUM(revenue_share_amt) as total_pre_deal_revenue,
        SUM(revenue_share_amt)/91 as daily_pre_deal_revenue,
        COUNT(DISTINCT customer_id) as total_pre_deal_customers,
        COUNT(DISTINCT customer_id)/91 as daily_pre_deal_customers,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END) as total_pre_deal_new_customers,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END) as total_pre_deal_return_customers,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/91 as daily_pre_deal_return_customers
    FROM pre_deal_daily_summary
    WHERE period_type = 'PRE_DEAL'
    GROUP BY 1,2,3
);

-- 3. Combine ASIN metrics with SNS data
DROP TABLE IF EXISTS asin_combined_metrics;
CREATE TEMP TABLE asin_combined_metrics AS (
    SELECT 
        d.*,
        p.total_pre_deal_shipped_units,
        p.daily_pre_deal_shipped_units,
        p.total_pre_deal_revenue,
        p.daily_pre_deal_revenue,
        p.total_pre_deal_customers,
        p.daily_pre_deal_customers,
        p.total_pre_deal_new_customers,
        p.daily_pre_deal_new_customers,
        p.total_pre_deal_return_customers,
        p.daily_pre_deal_return_customers,
        -- Add SNS metrics
        s.avg_deal_sns_subscribers as daily_deal_sns_subscribers,
        s.avg_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers
    FROM asin_deal_base d
        LEFT JOIN asin_pre_deal_base p
            ON d.asin = p.asin
            AND d.event_name = p.event_name
            AND d.event_year = p.event_year
        LEFT JOIN sns_metrics s
            ON d.asin = s.asin
            AND d.event_name = s.event_name
            AND d.event_year = s.event_year
);

-- 4. Add delta calculations
DROP TABLE IF EXISTS asin_metrics_with_deltas;
CREATE TEMP TABLE asin_metrics_with_deltas AS (
    SELECT 
        *,
        total_deal_shipped_units - total_pre_deal_shipped_units as delta_total_shipped_units,
        daily_deal_shipped_units - daily_pre_deal_shipped_units as delta_daily_shipped_units,
        total_deal_revenue - total_pre_deal_revenue as delta_total_revenue,
        daily_deal_revenue - daily_pre_deal_revenue as delta_daily_revenue,
        total_deal_customers - total_pre_deal_customers as delta_total_customers,
        daily_deal_customers - daily_pre_deal_customers as delta_daily_customers,
        total_deal_new_customers - total_pre_deal_new_customers as delta_total_new_customers,
        daily_deal_new_customers - daily_pre_deal_new_customers as delta_daily_new_customers,
        total_deal_return_customers - total_pre_deal_return_customers as delta_total_return_customers,
        daily_deal_return_customers - daily_pre_deal_return_customers as delta_daily_return_customers,
        daily_deal_sns_subscribers - daily_pre_deal_sns_subscribers as delta_daily_sns_subscribers
    FROM asin_combined_metrics
);

-- 5. Final ASIN metrics with last year comparison
DROP TABLE IF EXISTS final_asin_metrics;
CREATE TEMP TABLE final_asin_metrics AS (
    SELECT 
        curr.*,
        -- Last year metrics
        ly.total_deal_shipped_units as ly_total_deal_shipped_units,
        ly.daily_deal_shipped_units as ly_daily_deal_shipped_units,
        ly.total_deal_revenue as ly_total_deal_revenue,
        ly.daily_deal_revenue as ly_daily_deal_revenue,
        ly.total_deal_customers as ly_total_deal_customers,
        ly.daily_deal_customers as ly_daily_deal_customers,
        ly.total_deal_new_customers as ly_total_deal_new_customers,
        ly.daily_deal_new_customers as ly_daily_deal_new_customers,
        ly.total_deal_return_customers as ly_total_deal_return_customers,
        ly.daily_deal_return_customers as ly_daily_deal_return_customers,
        ly.total_pre_deal_shipped_units as ly_total_pre_deal_shipped_units,
        ly.daily_pre_deal_shipped_units as ly_daily_pre_deal_shipped_units,
        ly.total_pre_deal_revenue as ly_total_pre_deal_revenue,
        ly.daily_pre_deal_revenue as ly_daily_pre_deal_revenue,
        ly.total_pre_deal_customers as ly_total_pre_deal_customers,
        ly.daily_pre_deal_customers as ly_daily_pre_deal_customers,
        ly.total_pre_deal_new_customers as ly_total_pre_deal_new_customers,
        ly.daily_pre_deal_new_customers as ly_daily_pre_deal_new_customers,
        ly.total_pre_deal_return_customers as ly_total_pre_deal_return_customers,
        ly.daily_pre_deal_return_customers as ly_daily_pre_deal_return_customers,
        ly.daily_deal_sns_subscribers as ly_daily_deal_sns_subscribers,
        ly.daily_pre_deal_sns_subscribers as ly_daily_pre_deal_sns_subscribers
    FROM asin_metrics_with_deltas curr
        LEFT JOIN asin_metrics_with_deltas ly
            ON curr.asin = ly.asin
            AND curr.event_name = ly.event_name
            AND curr.event_year = ly.event_year + 1
);


-- 6. Create final event level table
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_event_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_event_level AS (
    SELECT 
        -- Event info
        event_name,
        event_year,
        event_duration_days,
        
        -- Deal Period Metrics
        total_deal_shipped_units,
        daily_deal_shipped_units,
        total_deal_revenue,
        daily_deal_revenue,
        total_deal_customers,
        daily_deal_customers,
        total_deal_new_customers,
        daily_deal_new_customers,
        total_deal_return_customers,
        daily_deal_return_customers,
        daily_deal_sns_subscribers,
        
        -- Pre-Deal Period Metrics
        total_pre_deal_shipped_units,
        daily_pre_deal_shipped_units,
        total_pre_deal_revenue,
        daily_pre_deal_revenue,
        total_pre_deal_customers,
        daily_pre_deal_customers,
        total_pre_deal_new_customers,
        daily_pre_deal_new_customers,
        total_pre_deal_return_customers,
        daily_pre_deal_return_customers,
        daily_pre_deal_sns_subscribers,
        
        -- Delta Metrics
        delta_total_shipped_units,
        delta_daily_shipped_units,
        delta_total_revenue,
        delta_daily_revenue,
        delta_total_customers,
        delta_daily_customers,
        delta_total_new_customers,
        delta_daily_new_customers,
        delta_total_return_customers,
        delta_daily_return_customers,
        delta_daily_sns_subscribers,
        
        -- Last Year Metrics
        ly_total_deal_shipped_units,
        ly_daily_deal_shipped_units,
        ly_total_deal_revenue,
        ly_daily_deal_revenue,
        ly_total_deal_customers,
        ly_daily_deal_customers,
        ly_total_deal_new_customers,
        ly_daily_deal_new_customers,
        ly_total_deal_return_customers,
        ly_daily_deal_return_customers,
        ly_total_pre_deal_shipped_units,
        ly_daily_pre_deal_shipped_units,
        ly_total_pre_deal_revenue,
        ly_daily_pre_deal_revenue,
        ly_total_pre_deal_customers,
        ly_daily_pre_deal_customers,
        ly_total_pre_deal_new_customers,
        ly_daily_pre_deal_new_customers,
        ly_total_pre_deal_return_customers,
        ly_daily_pre_deal_return_customers,
        ly_daily_deal_sns_subscribers,
        ly_daily_pre_deal_sns_subscribers
    FROM final_event_metrics
);


-- Grant permissions for all tables
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_asin_level TO PUBLIC;
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_brand_level TO PUBLIC;
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_company_level TO PUBLIC;
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_gl_level TO PUBLIC;