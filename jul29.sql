-- working on total/new customers for each level


/*************************
ntoes on SnS Metrics:
-- for sns metrics beyond ASIN level
-- use count(distinct customer_id) 
    FROM SUBS_SAVE_REPORTING.FCT_SNS_SALES_DETAILS_DAILY 
-- once subscription request approved
*************************/


/*************************
Base Orders Query
- Includes only retail merchant orders with shipped units > 0 
- Excludes cancelled or fraudulent orders
- Filters for last 730 days 
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
        mam.dama_mfg_vendor_code as vendor_code,
        v.company_code,
        v.company_name,
        (CASE 
            WHEN maa.gl_product_group = 510 THEN 'Lux Beauty'
            WHEN maa.gl_product_group = 364 THEN 'Personal Care Appliances'    
            WHEN maa.gl_product_group = 325 THEN 'Grocery'
            WHEN maa.gl_product_group = 199 THEN 'Pet'
            WHEN maa.gl_product_group = 194 THEN 'Beauty'
            WHEN maa.gl_product_group = 121 THEN 'HPC'
            WHEN maa.gl_product_group = 75 THEN 'Baby'    
        END) as gl_product_group_name
    FROM andes.booker.d_unified_cust_shipment_items o
        INNER JOIN andes.booker.d_mp_asin_attributes maa
            ON maa.asin = o.asin
            AND maa.marketplace_id = o.marketplace_id
            AND maa.region_id = o.region_id
            AND maa.gl_product_group IN (510, 364, 325, 199, 194, 121, 75)
        LEFT JOIN andes.contribution_ddl.o_wbr_cp_na cp
            ON o.customer_shipment_item_id = cp.customer_shipment_item_id 
            AND o.asin = cp.asin
        LEFT JOIN andes.BOOKER.D_MP_ASIN_MANUFACTURER mam
            ON mam.asin = o.asin
            AND mam.marketplace_id = 7
            AND mam.region_id = 1
        LEFT JOIN andes.roi_ml_ddl.VENDOR_COMPANY_CODES v
            ON v.vendor_code = mam.dama_mfg_vendor_code
    WHERE o.region_id = 1
        AND o.marketplace_id = 7
        AND o.shipped_units > 0
        AND o.is_retail_merchant = 'Y'
        AND o.order_datetime BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '730 days'
            AND TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
);


/*************************
SNS Metrics - daily aggregations
*************************/
DROP TABLE IF EXISTS daily_sns_metrics;
CREATE TEMP TABLE daily_sns_metrics AS (
    SELECT 
        asin,
        TO_DATE(snapshot_date, 'YYYY-MM-DD') as metric_date,
        AVG(active_subscription_count) as daily_sns_subscribers
    FROM andes.subs_save_ddl.d_daily_active_sns_asin_detail
    WHERE marketplace_id = 7
        AND gl_product_group IN (510, 364, 325, 199, 194, 121, 75)
        AND TO_DATE(snapshot_date, 'YYYY-MM-DD') >= current_date - interval '730 days'
    GROUP BY 
        asin,
        TO_DATE(snapshot_date, 'YYYY-MM-DD')
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
Deal Metrics - Base table with date ranges
*************************/
DROP TABLE IF EXISTS deal_base;
CREATE TEMP TABLE deal_base AS (
    SELECT DISTINCT
        asin,
        event_name,
        event_year,
        promo_start_date,
        promo_end_date,
        event_month,
        -- Deal period
        promo_start_date as deal_start_date,
        promo_end_date as deal_end_date,
        -- Pre-deal period (T-13W)
        promo_start_date - interval '91 day' as pre_deal_start_date,
        promo_start_date - interval '1 day' as pre_deal_end_date,
        -- Calculate event duration once
        (CASE 
            WHEN promo_end_date >= current_date
                THEN current_date - promo_start_date + 1
            ELSE promo_end_date - promo_start_date + 1
        END) AS event_duration_days
    FROM consolidated_promos
);


/*************************
Create unified base table combining orders and metrics
*************************/
DROP TABLE IF EXISTS unified_daily_metrics;
CREATE TEMP TABLE unified_daily_metrics AS (
    SELECT 
        -- Deal context
        d.asin,
        d.event_name,
        d.event_year,
        d.event_month,
        d.promo_start_date,
        d.promo_end_date,
        d.deal_start_date,
        d.deal_end_date,
        d.pre_deal_start_date,
        d.pre_deal_end_date,
        d.event_duration_days,
        
        -- Order date
        b.order_date,
        
        -- Period identifier
        CASE 
            WHEN b.order_date BETWEEN d.deal_start_date AND d.deal_end_date THEN 'DEAL'
            WHEN b.order_date BETWEEN d.pre_deal_start_date AND d.pre_deal_end_date THEN 'PRE_DEAL'
        END as period_type,
        
        -- Product/Business hierarchy
        b.item_name,
        b.gl_product_group,
        b.gl_product_group_name,
        b.brand_code,
        b.brand_name,
        b.vendor_code,
        b.company_code,
        b.company_name,
        
        -- Metrics
        b.customer_id,
        b.customer_shipment_item_id,
        b.shipped_units,
        b.revenue_share_amt,
        b.is_first_brand_purchase,
        COALESCE(s.daily_sns_subscribers, 0) as daily_sns_subscribers

    FROM deal_base d
        INNER JOIN base_orders b 
            ON d.asin = b.asin
            AND b.order_date BETWEEN d.pre_deal_start_date AND d.deal_end_date  -- covers both periods
        LEFT JOIN daily_sns_metrics s
            ON b.asin = s.asin
            AND b.order_date = s.metric_date
);



--------------------------- FIRST PURCHASES ---------------------------
-- creating these views separately to avoid double counting

/*************************
Create First Purchase flags for each level
*************************/
DROP TABLE IF EXISTS asin_first_purchases;
CREATE TEMP TABLE asin_first_purchases AS (
    SELECT 
        customer_id,
        asin,
        MIN(ship_day) as first_purchase_date
    FROM unified_daily_metrics
    WHERE asin IS NOT NULL
    GROUP BY customer_id, asin
);


-- Brand Level First Purchases
DROP TABLE IF EXISTS brand_first_purchases;
CREATE TEMP TABLE brand_first_purchases AS (
    SELECT 
        customer_id,
        brand_code,
        MIN(ship_day) as first_brand_purchase_date
    FROM unified_daily_metrics
    WHERE brand_code IS NOT NULL
    GROUP BY customer_id, brand_code
);

-- Company Level First Purchases
DROP TABLE IF EXISTS company_first_purchases;
CREATE TEMP TABLE company_first_purchases AS (
    SELECT 
        customer_id,
        company_code,
        gl_product_group, -- Including GL because we need company-GL combination
        MIN(ship_day) as first_company_gl_purchase_date
    FROM unified_daily_metrics
    WHERE company_code IS NOT NULL
    GROUP BY customer_id, company_code, gl_product_group
);

-- GL Level First Purchases
DROP TABLE IF EXISTS gl_first_purchases;
CREATE TEMP TABLE gl_first_purchases AS (
    SELECT 
        customer_id,
        gl_product_group,
        MIN(ship_day) as first_gl_purchase_date
    FROM unified_daily_metrics
    WHERE gl_product_group IS NOT NULL
    GROUP BY customer_id, gl_product_group
);


---------------------------------------- ASIN LEVEL ----------------------------------------

/*************************
Deal Period Metrics - Any Level (ASIN/Brand/Company/GL)
*************************/
DROP TABLE IF EXISTS deal_metrics;
CREATE TEMP TABLE deal_metrics AS (
    SELECT 
        -- Group by columns for desired level (e.g., ASIN level shown here)
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
        event_year,
        promo_start_date,
        promo_end_date,
        event_month,
        event_duration_days,
        
        -- Deal period metrics
        SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END)/event_duration_days as daily_deal_shipped_units,
        SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END)/event_duration_days as daily_deal_ops,
        COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END)/event_duration_days as daily_deal_customers,


        -- total customers
        COUNT(DISTINCT CASE 
            WHEN period_type = 'DEAL' 
            THEN udm.customer_id 
        END)/(MAX(promo_end_date) - MIN(promo_start_date) + 1) as daily_deal_customers,

        -- new customers
        COUNT(DISTINCT CASE 
            WHEN period_type = 'DEAL' 
            AND udm.ship_day = afp.first_brand_purchase_date -- This is the key change
            THEN udm.customer_id 
        END)/(MAX(promo_end_date) - MIN(promo_start_date) + 1) as daily_deal_new_customers,

        AVG(CASE WHEN period_type = 'DEAL' THEN daily_sns_subscribers END) as daily_deal_sns_subscribers
        
    FROM unified_daily_metrics udm

        LEFT JOIN asin_first_purchases afp
            ON udm.customer_id = afp.customer_id
            AND udm.asin = afp.asin  
        
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
        event_year,
        promo_start_date,
        promo_end_date,
        event_month,
        event_duration_days
);

/*************************
Pre-Deal Period Metrics
*************************/
DROP TABLE IF EXISTS pre_deal_metrics;
CREATE TEMP TABLE pre_deal_metrics AS (
    SELECT 
        asin,
        event_name,
        event_year,
        
        -- Pre-deal period metrics (always 91 days)
        SUM(CASE WHEN period_type = 'PRE_DEAL' 
            THEN shipped_units END)/91.0 as daily_pre_deal_shipped_units,
        SUM(CASE WHEN period_type = 'PRE_DEAL' 
            THEN revenue_share_amt END)/91.0 as daily_pre_deal_ops,
        COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' 
            THEN customer_id END)/91.0 as daily_pre_deal_customers,
        COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 
            THEN customer_id END)/91.0 as daily_pre_deal_new_customers,
        AVG(CASE WHEN period_type = 'PRE_DEAL' 
            THEN daily_sns_subscribers END) as daily_pre_deal_sns_subscribers
        
    FROM unified_daily_metrics
    WHERE period_type = 'PRE_DEAL'
    GROUP BY 1,2,3
);

/*************************
Combine & Calculate Deltas
*************************/
DROP TABLE IF EXISTS deal_growth;
CREATE TEMP TABLE deal_growth AS (
    SELECT 
        d.*,
        p.daily_pre_deal_shipped_units,
        p.daily_pre_deal_ops,
        p.daily_pre_deal_customers,
        p.daily_pre_deal_new_customers,
        p.daily_pre_deal_sns_subscribers,
        
        -- Delta calculations
        (d.daily_deal_shipped_units - p.daily_pre_deal_shipped_units) as delta_daily_shipped_units,
        (d.daily_deal_ops - p.daily_pre_deal_ops) as delta_daily_ops,
        (d.daily_deal_customers - p.daily_pre_deal_customers) as delta_daily_customers,
        (d.daily_deal_new_customers - p.daily_pre_deal_new_customers) as delta_daily_new_customers,
        (d.daily_deal_sns_subscribers - p.daily_ly_pre_deal_sns_subscribers) as delta_daily_sns_subscribers
    
    FROM deal_metrics d
        LEFT JOIN pre_deal_metrics p
            ON d.asin = p.asin
            AND d.event_name = p.event_name
            AND d.event_year = p.event_year
);

/*************************
Add Last Year Comparisons
*************************/
DROP TABLE IF EXISTS final_asin_metrics;
CREATE TEMP TABLE final_asin_metrics AS (
    SELECT
        t1.*,
        -- Last Year Metrics
        t2.daily_deal_shipped_units as ly_daily_deal_shipped_units,
        t2.daily_deal_ops as ly_daily_deal_ops,
        t2.daily_deal_customers as ly_daily_deal_customers,
        t2.daily_deal_new_customers as ly_daily_deal_new_customers,
        t2.daily_deal_sns_subscribers as ly_daily_deal_sns_subscribers,
        t2.daily_pre_deal_shipped_units as ly_daily_pre_deal_shipped_units,
        t2.daily_pre_deal_ops as ly_daily_pre_deal_ops,
        t2.daily_pre_deal_customers as ly_daily_pre_deal_customers,
        t2.daily_pre_deal_new_customers as ly_daily_pre_deal_new_customers,
        t2.daily_pre_deal_sns_subscribers as ly_daily_pre_deal_sns_subscribers
        
    FROM deal_growth t1
        LEFT JOIN deal_growth t2
            ON t1.asin = t2.asin
            AND t1.event_name = t2.event_name
            AND t1.event_year - 1 = t2.event_year
);

/*************************
Create Final Output Table
*************************/
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_asin_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_asin_level AS (
    SELECT 
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
        
        -- Current Year Deal Period Metrics
        daily_deal_shipped_units,
        daily_deal_ops,
        daily_deal_customers,
        daily_deal_new_customers,
        daily_deal_sns_subscribers,
        
        -- Current Year Pre-Deal Period Metrics
        daily_pre_deal_shipped_units,
        daily_pre_deal_ops,
        daily_pre_deal_customers,
        daily_pre_deal_new_customers,
        daily_pre_deal_sns_subscribers,
        
        -- Delta Metrics
        delta_daily_shipped_units,
        delta_daily_ops,
        delta_daily_customers,
        delta_daily_new_customers,
        delta_daily_sns_subscribers,
        
        -- Last Year Metrics
        ly_daily_deal_shipped_units,
        ly_daily_deal_ops,
        ly_daily_deal_customers,
        ly_daily_deal_new_customers,
        ly_daily_deal_sns_subscribers,
        ly_daily_pre_deal_shipped_units,
        ly_daily_pre_deal_ops,
        ly_daily_pre_deal_customers,
        ly_daily_pre_deal_new_customers,
        ly_daily_pre_deal_sns_subscribers

    FROM final_asin_metrics
    
    ORDER BY 
        event_year DESC,
        event_name,
        daily_deal_ops DESC
);

---------------------------------------- BRAND LEVEL --------------------------------------- 

/*************************
Brand Level Metrics (Corrected)
*************************/
DROP TABLE IF EXISTS deal_metrics_brand;
CREATE TEMP TABLE deal_metrics_brand AS (
    SELECT 
        brand_code,
        brand_name,
        vendor_code,
        company_code,
        company_name,
        gl_product_group,
        gl_product_group_name,
        event_name,
        event_year,
        event_month,
        MIN(promo_start_date) as promo_start_date,
        MAX(promo_end_date) as promo_end_date,
        MAX(promo_end_date) - MIN(promo_start_date) + 1 as event_duration_days,
        
        -- Deal period metrics - proper daily averages at brand level
        SUM(CASE WHEN period_type = 'DEAL' 
            THEN shipped_units END)/(MAX(promo_end_date) - MIN(promo_start_date) + 1) as daily_deal_shipped_units,
        SUM(CASE WHEN period_type = 'DEAL' 
            THEN revenue_share_amt END)/(MAX(promo_end_date) - MIN(promo_start_date) + 1) as daily_deal_ops,
       
       -- total customers
        COUNT(DISTINCT CASE 
            WHEN period_type = 'DEAL' 
            THEN udm.customer_id 
        END)/(MAX(promo_end_date) - MIN(promo_start_date) + 1) as daily_deal_customers,

        -- new customers
        COUNT(DISTINCT CASE 
            WHEN period_type = 'DEAL' 
            AND udm.ship_day = bfp.first_brand_purchase_date -- This is the key change
            THEN udm.customer_id 
        END)/(MAX(promo_end_date) - MIN(promo_start_date) + 1) as daily_deal_new_customers


        AVG(CASE WHEN period_type = 'DEAL' 
            THEN daily_sns_subscribers END) as daily_deal_sns_subscribers

    FROM unified_daily_metrics udm
        left join brand_first_purchases bfp
            ON udm.customer_id = bfp.customer_id
            AND udm.brand_code = bfp.brand_code 

    WHERE brand_code IS NOT NULL

    GROUP BY 
        brand_code,
        brand_name,
        vendor_code,
        company_code,
        company_name,
        gl_product_group,
        gl_product_group_name,
        event_name,
        event_year,
        event_month
);

-- Pre-deal period metrics for brand
DROP TABLE IF EXISTS pre_deal_metrics_brand;
CREATE TEMP TABLE pre_deal_metrics_brand AS (
    SELECT 
        brand_code,
        event_name,
        event_year,
        COUNT(DISTINCT asin) as participating_asins_pre_deal,
        
        -- Pre-deal period metrics - always 91 days
        SUM(CASE WHEN period_type = 'PRE_DEAL' 
            THEN shipped_units END)/91 as daily_pre_deal_shipped_units,
        SUM(CASE WHEN period_type = 'PRE_DEAL' 
            THEN revenue_share_amt END)/91.0 as daily_pre_deal_ops,
        COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' 
            THEN customer_id END)/91 as daily_pre_deal_customers,
        COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 
            THEN customer_id END)/91 as daily_pre_deal_new_customers,
        AVG(CASE WHEN period_type = 'PRE_DEAL' 
            THEN daily_sns_subscribers END) as daily_pre_deal_sns_subscribers
            
    FROM unified_daily_metrics
    WHERE brand_code IS NOT NULL
        AND period_type = 'PRE_DEAL'
    GROUP BY 
        brand_code,
        event_name,
        event_year
);

-- Combine and calculate deltas for brand
DROP TABLE IF EXISTS deal_growth_brand;
CREATE TEMP TABLE deal_growth_brand AS (
    SELECT 
        d.*,
        p.participating_asins_pre_deal,
        p.daily_pre_deal_shipped_units,
        p.daily_pre_deal_ops,
        p.daily_pre_deal_customers,
        p.daily_pre_deal_new_customers,
        p.daily_pre_deal_sns_subscribers,
        
        -- Delta calculations at brand level
        (d.daily_deal_shipped_units - p.daily_pre_deal_shipped_units) as delta_daily_shipped_units,
        (d.daily_deal_ops - p.daily_pre_deal_ops) as delta_daily_ops,
        (d.daily_deal_customers - p.daily_pre_deal_customers) as delta_daily_customers,
        (d.daily_deal_new_customers - p.daily_pre_deal_new_customers) as delta_daily_new_customers,
        (d.daily_deal_sns_subscribers - p.daily_pre_deal_sns_subscribers) as delta_daily_sns_subscribers
    
    FROM deal_metrics_brand d
        LEFT JOIN pre_deal_metrics_brand p
            ON d.brand_code = p.brand_code
            AND d.event_name = p.event_name
            AND d.event_year = p.event_year
);


/*************************
Calculate Last Year metrics for Brand Level
*************************/
DROP TABLE IF EXISTS final_brand_metrics;
CREATE TEMP TABLE final_brand_metrics AS (
    SELECT
        t1.*,
        -- Last Year Deal Period Metrics
        COALESCE(t2.daily_deal_shipped_units, 0) as ly_daily_deal_shipped_units,
        COALESCE(t2.daily_deal_ops, 0) as ly_daily_deal_ops,
        COALESCE(t2.daily_deal_customers, 0) as ly_daily_deal_customers,
        COALESCE(t2.daily_deal_new_customers, 0) as ly_daily_deal_new_customers,
        COALESCE(t2.daily_deal_sns_subscribers, 0) as ly_daily_deal_sns_subscribers,
        -- Last Year Pre-Deal Period Metrics
        COALESCE(t2.daily_pre_deal_shipped_units, 0) as ly_daily_pre_deal_shipped_units,
        COALESCE(t2.daily_pre_deal_ops, 0) as ly_daily_pre_deal_ops,
        COALESCE(t2.daily_pre_deal_customers, 0) as ly_daily_pre_deal_customers,
        COALESCE(t2.daily_pre_deal_new_customers, 0) as ly_daily_pre_deal_new_customers,
        COALESCE(t2.daily_pre_deal_sns_subscribers, 0) as ly_daily_pre_deal_sns_subscribers

    FROM deal_growth_brand t1
        LEFT JOIN deal_growth_brand t2
            ON t1.brand_code = t2.brand_code
            AND t1.event_name = t2.event_name
            AND t1.event_year - 1 = t2.event_year

    WHERE t1.event_name IS NOT NULL
);


/*************************
Create Final Output Table for Brand Level with NULL handling
*************************/
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_brand_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_brand_level AS (
    SELECT 
        brand_code,
        brand_name,
        vendor_code,
        company_code,
        company_name,
        gl_product_group,
        gl_product_group_name,
        event_name,
        event_year,
        event_month,
        promo_start_date,
        promo_end_date,
        event_duration_days,
        
        -- Current Year Deal Period Metrics
        COALESCE(daily_deal_shipped_units, 0) as daily_deal_shipped_units,
        COALESCE(daily_deal_ops, 0) as daily_deal_ops,
        COALESCE(daily_deal_customers, 0) as daily_deal_customers,
        COALESCE(daily_deal_new_customers, 0) as daily_deal_new_customers,
        COALESCE(daily_deal_sns_subscribers, 0) as daily_deal_sns_subscribers,
        
        -- Current Year Pre-Deal Period Metrics
        COALESCE(daily_pre_deal_shipped_units, 0) as daily_pre_deal_shipped_units,
        COALESCE(daily_pre_deal_ops, 0) as daily_pre_deal_ops,
        COALESCE(daily_pre_deal_customers, 0) as daily_pre_deal_customers,
        COALESCE(daily_pre_deal_new_customers, 0) as daily_pre_deal_new_customers,
        COALESCE(daily_pre_deal_sns_subscribers, 0) as daily_pre_deal_sns_subscribers,
        
        -- Delta Metrics
        COALESCE(delta_daily_shipped_units, 0) as delta_daily_shipped_units,
        COALESCE(delta_daily_ops, 0) as delta_daily_ops,
        COALESCE(delta_daily_customers, 0) as delta_daily_customers,
        COALESCE(delta_daily_new_customers, 0) as delta_daily_new_customers,
        COALESCE(delta_daily_sns_subscribers, 0) as delta_daily_sns_subscribers,
        
        -- Last Year Metrics
        COALESCE(ly_daily_deal_shipped_units, 0) as ly_daily_deal_shipped_units,
        COALESCE(ly_daily_deal_ops, 0) as ly_daily_deal_ops,
        COALESCE(ly_daily_deal_customers, 0) as ly_daily_deal_customers,
        COALESCE(ly_daily_deal_new_customers, 0) as ly_daily_deal_new_customers,
        COALESCE(ly_daily_deal_sns_subscribers, 0) as ly_daily_deal_sns_subscribers,
        COALESCE(ly_daily_pre_deal_shipped_units, 0) as ly_daily_pre_deal_shipped_units,
        COALESCE(ly_daily_pre_deal_ops, 0) as ly_daily_pre_deal_ops,
        COALESCE(ly_daily_pre_deal_customers, 0) as ly_daily_pre_deal_customers,
        COALESCE(ly_daily_pre_deal_new_customers, 0) as ly_daily_pre_deal_new_customers,
        COALESCE(ly_daily_pre_deal_sns_subscribers, 0) as ly_daily_pre_deal_sns_subscribers

    FROM final_brand_metrics
    ORDER BY 
        event_year DESC,
        event_name,
        daily_deal_ops DESC
);

---------------------------------------- COMPANY-GL LEVEL ----------------------------------------

/*************************
Company-GL Level Metrics
*************************/
DROP TABLE IF EXISTS deal_metrics_company;
CREATE TEMP TABLE deal_metrics_company AS (
    SELECT 
        company_code,
        company_name,
        gl_product_group,
        gl_product_group_name,
        event_name,
        event_year,
        event_month,
        MIN(promo_start_date) as promo_start_date,
        MAX(promo_end_date) as promo_end_date,
        MAX(promo_end_date) - MIN(promo_start_date) + 1 as event_duration_days,
        
        -- Deal period metrics - proper daily averages at company-GL level
        SUM(CASE WHEN period_type = 'DEAL' 
            THEN shipped_units END)/(MAX(promo_end_date) - MIN(promo_start_date) + 1) as daily_deal_shipped_units,
        SUM(CASE WHEN period_type = 'DEAL' 
            THEN revenue_share_amt END)/(MAX(promo_end_date) - MIN(promo_start_date) + 1) as daily_deal_ops,
        COUNT(DISTINCT CASE WHEN period_type = 'DEAL' 
            THEN customer_id END)/(MAX(promo_end_date) - MIN(promo_start_date) + 1) as daily_deal_customers,
        COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 
            THEN customer_id END)/(MAX(promo_end_date) - MIN(promo_start_date) + 1) as daily_deal_new_customers,
        AVG(CASE WHEN period_type = 'DEAL' 
            THEN daily_sns_subscribers END) as daily_deal_sns_subscribers

    FROM unified_daily_metrics
    WHERE company_code IS NOT NULL
    GROUP BY 
        company_code,
        company_name,
        gl_product_group,
        gl_product_group_name,
        event_name,
        event_year,
        event_month
);

-- Pre-deal period metrics for company-GL
DROP TABLE IF EXISTS pre_deal_metrics_company;
CREATE TEMP TABLE pre_deal_metrics_company AS (
    SELECT 
        company_code,
        gl_product_group,
        event_name,
        event_year,
        
        -- Pre-deal period metrics - always 91 days
        SUM(CASE WHEN period_type = 'PRE_DEAL' 
            THEN shipped_units END)/91 as daily_pre_deal_shipped_units,
        SUM(CASE WHEN period_type = 'PRE_DEAL' 
            THEN revenue_share_amt END)/91.0 as daily_pre_deal_ops,
        COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' 
            THEN customer_id END)/91 as daily_pre_deal_customers,
        COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 
            THEN customer_id END)/91 as daily_pre_deal_new_customers,
        AVG(CASE WHEN period_type = 'PRE_DEAL' 
            THEN daily_sns_subscribers END) as daily_pre_deal_sns_subscribers
            
    FROM unified_daily_metrics

    WHERE company_code IS NOT NULL
        AND period_type = 'PRE_DEAL'
        
    GROUP BY 

        company_code,
        gl_product_group,
        event_name,
        event_year
);

-- Combine and calculate deltas for company-GL
DROP TABLE IF EXISTS deal_growth_company;
CREATE TEMP TABLE deal_growth_company AS (
    SELECT 
        d.*,
        p.participating_asins_pre_deal,
        p.daily_pre_deal_shipped_units,
        p.daily_pre_deal_ops,
        p.daily_pre_deal_customers,
        p.daily_pre_deal_new_customers,
        p.daily_pre_deal_sns_subscribers,
        
        -- Delta calculations at company-GL level
        (d.daily_deal_shipped_units - p.daily_pre_deal_shipped_units) as delta_daily_shipped_units,
        (d.daily_deal_ops - p.daily_pre_deal_ops) as delta_daily_ops,
        (d.daily_deal_customers - p.daily_pre_deal_customers) as delta_daily_customers,
        (d.daily_deal_new_customers - p.daily_pre_deal_new_customers) as delta_daily_new_customers,
        (d.daily_deal_sns_subscribers - p.daily_pre_deal_sns_subscribers) as delta_daily_sns_subscribers
    
    FROM deal_metrics_company d
        LEFT JOIN pre_deal_metrics_company p
            ON d.company_code = p.company_code
            AND d.gl_product_group = p.gl_product_group
            AND d.event_name = p.event_name
            AND d.event_year = p.event_year
);


/*************************
Calculate Last Year metrics for Company-GL Level
*************************/

DROP TABLE IF EXISTS final_company_metrics;
CREATE TEMP TABLE final_company_metrics AS (
    SELECT
        t1.*,
        -- Last Year Deal Period Metrics
        COALESCE(t2.daily_deal_shipped_units, 0) as ly_daily_deal_shipped_units,
        COALESCE(t2.daily_deal_ops, 0) as ly_daily_deal_ops,
        COALESCE(t2.daily_deal_customers, 0) as ly_daily_deal_customers,
        COALESCE(t2.daily_deal_new_customers, 0) as ly_daily_deal_new_customers,
        COALESCE(t2.daily_deal_sns_subscribers, 0) as ly_daily_deal_sns_subscribers,
        -- Last Year Pre-Deal Period Metrics
        COALESCE(t2.daily_pre_deal_shipped_units, 0) as ly_daily_pre_deal_shipped_units,
        COALESCE(t2.daily_pre_deal_ops, 0) as ly_daily_pre_deal_ops,
        COALESCE(t2.daily_pre_deal_customers, 0) as ly_daily_pre_deal_customers,
        COALESCE(t2.daily_pre_deal_new_customers, 0) as ly_daily_pre_deal_new_customers,
        COALESCE(t2.daily_pre_deal_sns_subscribers, 0) as ly_daily_pre_deal_sns_subscribers

    FROM deal_growth_company t1
        LEFT JOIN deal_growth_company t2
            ON t1.company_code = t2.company_code
            AND t1.gl_product_group = t2.gl_product_group
            AND t1.event_name = t2.event_name
            AND t1.event_year - 1 = t2.event_year

    WHERE t1.company_code IS NOT NULL
        -- AND t1.event_name IS NOT NULL
    
);

/*************************
Create Final Output Table for Company-GL Level
*************************/
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_company_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_company_level AS (
    SELECT 
        company_code,
        company_name,
        gl_product_group,
        gl_product_group_name,
        event_name,
        event_year,
        event_month,
        promo_start_date,
        promo_end_date,
        event_duration_days,
        
        -- Current Year Deal Period Metrics
        COALESCE(daily_deal_shipped_units, 0) as daily_deal_shipped_units,
        COALESCE(daily_deal_ops, 0) as daily_deal_ops,
        COALESCE(daily_deal_customers, 0) as daily_deal_customers,
        COALESCE(daily_deal_new_customers, 0) as daily_deal_new_customers,
        COALESCE(daily_deal_sns_subscribers, 0) as daily_deal_sns_subscribers,
        
        -- Current Year Pre-Deal Period Metrics
        COALESCE(daily_pre_deal_shipped_units, 0) as daily_pre_deal_shipped_units,
        COALESCE(daily_pre_deal_ops, 0) as daily_pre_deal_ops,
        COALESCE(daily_pre_deal_customers, 0) as daily_pre_deal_customers,
        COALESCE(daily_pre_deal_new_customers, 0) as daily_pre_deal_new_customers,
        COALESCE(daily_pre_deal_sns_subscribers, 0) as daily_pre_deal_sns_subscribers,
        
        -- Delta Metrics
        COALESCE(delta_daily_shipped_units, 0) as delta_daily_shipped_units,
        COALESCE(delta_daily_ops, 0) as delta_daily_ops,
        COALESCE(delta_daily_customers, 0) as delta_daily_customers,
        COALESCE(delta_daily_new_customers, 0) as delta_daily_new_customers,
        COALESCE(delta_daily_sns_subscribers, 0) as delta_daily_sns_subscribers,
        
        -- Last Year Metrics
        COALESCE(ly_daily_deal_shipped_units, 0) as ly_daily_deal_shipped_units,
        COALESCE(ly_daily_deal_ops, 0) as ly_daily_deal_ops,
        COALESCE(ly_daily_deal_customers, 0) as ly_daily_deal_customers,
        COALESCE(ly_daily_deal_new_customers, 0) as ly_daily_deal_new_customers,
        COALESCE(ly_daily_deal_sns_subscribers, 0) as ly_daily_deal_sns_subscribers,
        COALESCE(ly_daily_pre_deal_shipped_units, 0) as ly_daily_pre_deal_shipped_units,
        COALESCE(ly_daily_pre_deal_ops, 0) as ly_daily_pre_deal_ops,
        COALESCE(ly_daily_pre_deal_customers, 0) as ly_daily_pre_deal_customers,
        COALESCE(ly_daily_pre_deal_new_customers, 0) as ly_daily_pre_deal_new_customers,
        COALESCE(ly_daily_pre_deal_sns_subscribers, 0) as ly_daily_pre_deal_sns_subscribers

    FROM final_company_metrics
    ORDER BY 
        event_year DESC,
        event_name,
        daily_deal_ops DESC
);


-------------------------------- GL LEVELS ----------------------------------------
/*************************
GL Level Metrics
*************************/
DROP TABLE IF EXISTS deal_metrics_gl;
CREATE TEMP TABLE deal_metrics_gl AS (
    SELECT 
        gl_product_group,
        gl_product_group_name,
        event_name,
        event_year,
        event_month,
        MIN(promo_start_date) as promo_start_date,
        MAX(promo_end_date) as promo_end_date,
        MAX(promo_end_date) - MIN(promo_start_date) + 1 as event_duration_days,
        
        -- Deal period metrics - proper daily averages at GL level
        SUM(CASE WHEN period_type = 'DEAL' 
            THEN shipped_units END)/(MAX(promo_end_date) - MIN(promo_start_date) + 1) as daily_deal_shipped_units,
        SUM(CASE WHEN period_type = 'DEAL' 
            THEN revenue_share_amt END)/(MAX(promo_end_date) - MIN(promo_start_date) + 1) as daily_deal_ops,
        COUNT(DISTINCT CASE WHEN period_type = 'DEAL' 
            THEN customer_id END)/(MAX(promo_end_date) - MIN(promo_start_date) + 1) as daily_deal_customers,
        COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 
            THEN customer_id END)/(MAX(promo_end_date) - MIN(promo_start_date) + 1) as daily_deal_new_customers,
        AVG(CASE WHEN period_type = 'DEAL' 
            THEN daily_sns_subscribers END) as daily_deal_sns_subscribers

    FROM unified_daily_metrics
    WHERE gl_product_group IS NOT NULL
    GROUP BY 
        gl_product_group,
        gl_product_group_name,
        event_name,
        event_year,
        event_month
);

-- Pre-deal period metrics for GL
DROP TABLE IF EXISTS pre_deal_metrics_gl;
CREATE TEMP TABLE pre_deal_metrics_gl AS (
    SELECT 
        gl_product_group,
        event_name,
        event_year,
        
        -- Pre-deal period metrics - always 91 days
        SUM(CASE WHEN period_type = 'PRE_DEAL' 
            THEN shipped_units END)/91 as daily_pre_deal_shipped_units,
        SUM(CASE WHEN period_type = 'PRE_DEAL' 
            THEN revenue_share_amt END)/91.0 as daily_pre_deal_ops,
        COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' 
            THEN customer_id END)/91 as daily_pre_deal_customers,
        COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 
            THEN customer_id END)/91 as daily_pre_deal_new_customers,
        AVG(CASE WHEN period_type = 'PRE_DEAL' 
            THEN daily_sns_subscribers END) as daily_pre_deal_sns_subscribers
            
    FROM unified_daily_metrics
    WHERE gl_product_group IS NOT NULL
        AND period_type = 'PRE_DEAL'
    GROUP BY 
        gl_product_group,
        event_name,
        event_year
);

-- Combine and calculate deltas for GL
DROP TABLE IF EXISTS deal_growth_gl;
CREATE TEMP TABLE deal_growth_gl AS (
    SELECT 
        d.*,
        p.daily_pre_deal_shipped_units,
        p.daily_pre_deal_ops,
        p.daily_pre_deal_customers,
        p.daily_pre_deal_new_customers,
        p.daily_pre_deal_sns_subscribers,
        
        -- Delta calculations at GL level
        (d.daily_deal_shipped_units - p.daily_pre_deal_shipped_units) as delta_daily_shipped_units,
        (d.daily_deal_ops - p.daily_pre_deal_ops) as delta_daily_ops,
        (d.daily_deal_customers - p.daily_pre_deal_customers) as delta_daily_customers,
        (d.daily_deal_new_customers - p.daily_pre_deal_new_customers) as delta_daily_new_customers,
        (d.daily_deal_sns_subscribers - p.daily_pre_deal_sns_subscribers) as delta_daily_sns_subscribers
    
    FROM deal_metrics_gl d
        LEFT JOIN pre_deal_metrics_gl p
            ON d.gl_product_group = p.gl_product_group
            AND d.event_name = p.event_name
            AND d.event_year = p.event_year
);

/*************************
Calculate Last Year metrics for GL Level
*************************/
DROP TABLE IF EXISTS final_gl_metrics;
CREATE TEMP TABLE final_gl_metrics AS (
    SELECT
        t1.*,
        -- Last Year Deal Period Metrics
        COALESCE(t2.daily_deal_shipped_units, 0) as ly_daily_deal_shipped_units,
        COALESCE(t2.daily_deal_ops, 0) as ly_daily_deal_ops,
        COALESCE(t2.daily_deal_customers, 0) as ly_daily_deal_customers,
        COALESCE(t2.daily_deal_new_customers, 0) as ly_daily_deal_new_customers,
        COALESCE(t2.daily_deal_sns_subscribers, 0) as ly_daily_deal_sns_subscribers,
        -- Last Year Pre-Deal Period Metrics
        COALESCE(t2.daily_pre_deal_shipped_units, 0) as ly_daily_pre_deal_shipped_units,
        COALESCE(t2.daily_pre_deal_ops, 0) as ly_daily_pre_deal_ops,
        COALESCE(t2.daily_pre_deal_customers, 0) as ly_daily_pre_deal_customers,
        COALESCE(t2.daily_pre_deal_new_customers, 0) as ly_daily_pre_deal_new_customers,
        COALESCE(t2.daily_pre_deal_sns_subscribers, 0) as ly_daily_pre_deal_sns_subscribers

    FROM deal_growth_gl t1
        LEFT JOIN deal_growth_gl t2
            ON t1.gl_product_group = t2.gl_product_group
            AND t1.event_name = t2.event_name
            AND t1.event_year - 1 = t2.event_year

    WHERE t1.event_name IS NOT NULL
);

/*************************
Create Final Output Table for GL Level
*************************/
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_gl_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_gl_level AS (
    SELECT 
        gl_product_group,
        gl_product_group_name,
        event_name,
        event_year,
        event_month,
        promo_start_date,
        promo_end_date,
        event_duration_days,
        
        -- Current Year Deal Period Metrics
        COALESCE(daily_deal_shipped_units, 0) as daily_deal_shipped_units,
        COALESCE(daily_deal_ops, 0) as daily_deal_ops,
        COALESCE(daily_deal_customers, 0) as daily_deal_customers,
        COALESCE(daily_deal_new_customers, 0) as daily_deal_new_customers,
        COALESCE(daily_deal_sns_subscribers, 0) as daily_deal_sns_subscribers,
        
        -- Current Year Pre-Deal Period Metrics
        COALESCE(daily_pre_deal_shipped_units, 0) as daily_pre_deal_shipped_units,
        COALESCE(daily_pre_deal_ops, 0) as daily_pre_deal_ops,
        COALESCE(daily_pre_deal_customers, 0) as daily_pre_deal_customers,
        COALESCE(daily_pre_deal_new_customers, 0) as daily_pre_deal_new_customers,
        COALESCE(daily_pre_deal_sns_subscribers, 0) as daily_pre_deal_sns_subscribers,
        
        -- Delta Metrics
        COALESCE(delta_daily_shipped_units, 0) as delta_daily_shipped_units,
        COALESCE(delta_daily_ops, 0) as delta_daily_ops,
        COALESCE(delta_daily_customers, 0) as delta_daily_customers,
        COALESCE(delta_daily_new_customers, 0) as delta_daily_new_customers,
        COALESCE(delta_daily_sns_subscribers, 0) as delta_daily_sns_subscribers,
        
        -- Last Year Metrics
        COALESCE(ly_daily_deal_shipped_units, 0) as ly_daily_deal_shipped_units,
        COALESCE(ly_daily_deal_ops, 0) as ly_daily_deal_ops,
        COALESCE(ly_daily_deal_customers, 0) as ly_daily_deal_customers,
        COALESCE(ly_daily_deal_new_customers, 0) as ly_daily_deal_new_customers,
        COALESCE(ly_daily_deal_sns_subscribers, 0) as ly_daily_deal_sns_subscribers,
        COALESCE(ly_daily_pre_deal_shipped_units, 0) as ly_daily_pre_deal_shipped_units,
        COALESCE(ly_daily_pre_deal_ops, 0) as ly_daily_pre_deal_ops,
        COALESCE(ly_daily_pre_deal_customers, 0) as ly_daily_pre_deal_customers,
        COALESCE(ly_daily_pre_deal_new_customers, 0) as ly_daily_pre_deal_new_customers,
        COALESCE(ly_daily_pre_deal_sns_subscribers, 0) as ly_daily_pre_deal_sns_subscribers

    FROM final_gl_metrics
    ORDER BY 
        event_year DESC,
        event_name,
        daily_deal_ops DESC
);





-- Grant permissions for all tables
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_asin_level TO PUBLIC;
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_brand_level TO PUBLIC;
-- GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_company_level TO PUBLIC;
-- GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_gl_level TO PUBLIC;
-- GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_event_level TO PUBLIC;
