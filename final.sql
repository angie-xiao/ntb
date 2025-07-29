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
DROP TABLE IF EXISTS sns_metrics1;
CREATE TEMP TABLE sns_metrics1 AS (
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


DROP TABLE IF EXISTS sns_metrics;
CREATE TEMP TABLE sns_metrics AS (
    SELECT 
        sns.asin,
        b.brand_code,
        b.brand_name,
        b.vendor_code,
        b.company_code,
        b.company_name,
        b.gl_product_group,
        b.gl_product_group_name,
        sns.event_name,
        sns.event_year,
        avg_deal_sns_subscribers,
        avg_pre_deal_sns_subscribers
        
    FROM sns_metrics1 sns 
        LEFT JOIN base_orders b
        ON sns.asin = b.asin
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
DROP TABLE IF EXISTS deal_metrics_brand;
CREATE TEMP TABLE deal_metrics_brand AS (
    SELECT 
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
        event_year,
        event_month,
        event_duration_days,
        SUM(shipped_units)/event_duration_days as daily_deal_shipped_units,
        SUM(revenue_share_amt)/event_duration_days as daily_deal_ops,
        COUNT(DISTINCT customer_id)/event_duration_days as daily_deal_total_customers_brand,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/event_duration_days as daily_deal_new_customers_brand,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/event_duration_days as daily_deal_return_customers_brand
    FROM deal_daily_summary
    GROUP BY 
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
        event_year,
        event_month,
        event_duration_days
);

DROP TABLE IF EXISTS pre_deal_metrics_brand;
CREATE TEMP TABLE pre_deal_metrics_brand AS (
    SELECT 
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
        event_year,
        event_month,
        event_duration_days,
        SUM(shipped_units)/91 as daily_pre_deal_shipped_units,
        SUM(revenue_share_amt)/91 as daily_pre_deal_revenue,
        COUNT(DISTINCT customer_id)/91 as daily_pre_deal_total_customers_brand,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers_brand
    FROM pre_deal_daily_summary
    GROUP BY 
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
        event_year,
        event_month,
        event_duration_days
);

/*************************
Compare deal vs pre deal periods (delta) - Brand Level
*************************/
DROP TABLE IF EXISTS deal_growth_brand;
CREATE TEMP TABLE deal_growth_brand AS (
    SELECT 
        -- brand info
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
        
        -- during deal daily avg (Brand level)
        d.daily_deal_shipped_units,
        d.daily_deal_ops,    
        d.daily_deal_total_customers_brand,
        d.daily_deal_new_customers_brand,
        d.daily_deal_return_customers_brand,
        -- sns
        s.avg_deal_sns_subscribers as daily_deal_sns_subscribers,

        -- pre deal daily avg (Brand level)
        p.daily_pre_deal_shipped_units,
        p.daily_pre_deal_revenue,
        p.daily_pre_deal_total_customers_brand,
        p.daily_pre_deal_new_customers_brand,
        -- sns
        s.avg_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers,

        -- Calculate Delta Metrics
        (d.daily_deal_shipped_units - p.daily_pre_deal_shipped_units) as delta_daily_shipped_units_brand,
        (d.daily_deal_ops - p.daily_pre_deal_revenue) as delta_daily_revenue_brand,
        (d.daily_deal_total_customers_brand - p.daily_pre_deal_total_customers_brand) as delta_daily_customers_brand,
        (d.daily_deal_new_customers_brand - p.daily_pre_deal_new_customers_brand) as delta_daily_new_customers_brand,
        (s.avg_deal_sns_subscribers - s.avg_pre_deal_sns_subscribers) as delta_daily_sns_subscribers_brand

    FROM deal_metrics_brand d
        LEFT JOIN pre_deal_metrics_brand p
            ON d.brand_code = p.brand_code
            AND d.event_name = p.event_name
            AND d.event_year = p.event_year 
        LEFT JOIN (
            SELECT 
                brand_code,
                event_name,
                event_year,
                AVG(avg_deal_sns_subscribers) as avg_deal_sns_subscribers,
                AVG(avg_pre_deal_sns_subscribers) as avg_pre_deal_sns_subscribers
            FROM sns_metrics
            GROUP BY brand_code, event_name, event_year
        ) s
            ON d.brand_code = s.brand_code
            AND d.event_name = s.event_name
            AND d.event_year = s.event_year
    
    WHERE d.promo_start_date IS NOT NULL
    ORDER BY 
        d.promo_start_date DESC,
        d.daily_deal_ops * d.event_duration_days DESC
);

/*************************
Calculate Last Year metrics - Brand Level
*************************/
DROP TABLE IF EXISTS final_brand_metrics;
CREATE TEMP TABLE final_brand_metrics AS (
    SELECT
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
        t1.daily_deal_shipped_units as daily_deal_shipped_units_brand,
        t1.daily_deal_ops as daily_deal_revenue_brand,
        t1.daily_deal_total_customers_brand as daily_deal_customers_brand,
        t1.daily_deal_new_customers_brand,
        t1.daily_deal_return_customers_brand,
        t1.daily_deal_sns_subscribers as daily_deal_sns_subscribers_brand,

        -- Current Year Pre-Deal Period Metrics
        t1.daily_pre_deal_shipped_units as daily_pre_deal_shipped_units_brand,
        t1.daily_pre_deal_revenue as daily_pre_deal_revenue_brand,
        t1.daily_pre_deal_total_customers_brand as daily_pre_deal_customers_brand,
        t1.daily_pre_deal_new_customers_brand,
        t1.daily_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers_brand,

        -- Last Year Deal Period Metrics
        t2.daily_deal_shipped_units as ly_daily_deal_shipped_units_brand,
        t2.daily_deal_ops as ly_daily_deal_revenue_brand,
        t2.daily_deal_total_customers_brand as ly_daily_deal_customers_brand,
        t2.daily_deal_new_customers_brand as ly_daily_deal_new_customers_brand,
        t2.daily_deal_return_customers_brand as ly_daily_deal_return_customers_brand,
        t2.daily_deal_sns_subscribers as ly_daily_deal_sns_subscribers_brand,

        -- Last Year Pre-Deal Period Metrics
        t2.daily_pre_deal_shipped_units as ly_daily_pre_deal_shipped_units_brand,
        t2.daily_pre_deal_revenue as ly_daily_pre_deal_revenue_brand,
        t2.daily_pre_deal_total_customers_brand as ly_daily_pre_deal_customers_brand,
        t2.daily_pre_deal_new_customers_brand as ly_daily_pre_deal_new_customers_brand,
        t2.daily_pre_deal_sns_subscribers as ly_daily_pre_deal_sns_subscribers_brand,

        -- Delta Metrics
        t1.delta_daily_shipped_units_brand,
        t1.delta_daily_revenue_brand,
        t1.delta_daily_customers_brand,
        t1.delta_daily_new_customers_brand,
        t1.delta_daily_sns_subscribers_brand

    FROM deal_growth_brand t1
        LEFT JOIN deal_growth_brand t2
        ON t1.brand_code = t2.brand_code
        AND t1.event_name = t2.event_name
        AND t1.event_year - 1 = t2.event_year
    WHERE t1.event_name IS NOT NULL
);

/*************************
# 2. FINAL TABLE CREATION
-- Brand Level 
*************************/
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_brand_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_brand_level AS (
    SELECT 
        -- Base Brand info
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
        daily_pre_deal_sns_subscribers_brand,
        
        -- Delta Metrics
        delta_daily_shipped_units_brand,
        delta_daily_revenue_brand,
        delta_daily_customers_brand,
        delta_daily_new_customers_brand,
        delta_daily_sns_subscribers_brand,
        
        -- Last Year Metrics
        ly_daily_deal_shipped_units_brand,
        ly_daily_deal_revenue_brand,
        ly_daily_deal_customers_brand,
        ly_daily_deal_new_customers_brand,
        ly_daily_deal_return_customers_brand,
        ly_daily_deal_sns_subscribers_brand,
        ly_daily_pre_deal_shipped_units_brand,
        ly_daily_pre_deal_revenue_brand,
        ly_daily_pre_deal_customers_brand,
        ly_daily_pre_deal_new_customers_brand,
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
# 1. FINAL TABLE CREATION
-- ASIN Level 
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

-- Pre-deal daily summary
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

-- Deal metrics calculation (daily avg)
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

-- Pre-deal metrics calculation
-- Include event context
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
-- Compare deal vs pre deal periods (delta)
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


-- Calculate Last Year metrics
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

-- final output ASIN level
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
# 2. FINAL TABLE CREATION
-- Brand Level 
*************************/

DROP TABLE IF EXISTS deal_metrics_brand;
CREATE TEMP TABLE deal_metrics_brand AS (
    SELECT 
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
        event_year,
        event_month,
        event_duration_days,
        SUM(shipped_units)/event_duration_days as daily_deal_shipped_units,
        SUM(revenue_share_amt)/event_duration_days as daily_deal_ops,
        COUNT(DISTINCT customer_id)/event_duration_days as daily_deal_total_customers_brand,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/event_duration_days as daily_deal_new_customers_brand,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/event_duration_days as daily_deal_return_customers_brand
    FROM deal_daily_summary
    GROUP BY 
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
        event_year,
        event_month,
        event_duration_days
);

DROP TABLE IF EXISTS pre_deal_metrics_brand;
CREATE TEMP TABLE pre_deal_metrics_brand AS (
    SELECT 
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
        event_year,
        event_month,
        event_duration_days,
        SUM(shipped_units)/91 as daily_pre_deal_shipped_units,
        SUM(revenue_share_amt)/91 as daily_pre_deal_revenue,
        COUNT(DISTINCT customer_id)/91 as daily_pre_deal_total_customers_brand,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers_brand
    FROM pre_deal_daily_summary
    GROUP BY 
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
        event_year,
        event_month,
        event_duration_days
);


-- Compare deal vs pre deal periods (delta) - Brand Level
DROP TABLE IF EXISTS deal_growth_brand;
CREATE TEMP TABLE deal_growth_brand AS (
    SELECT 
        -- brand info
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
        
        -- during deal daily avg (Brand level)
        d.daily_deal_shipped_units,
        d.daily_deal_ops,    
        d.daily_deal_total_customers_brand,
        d.daily_deal_new_customers_brand,
        d.daily_deal_return_customers_brand,
        -- sns
        s.avg_deal_sns_subscribers as daily_deal_sns_subscribers,

        -- pre deal daily avg (Brand level)
        p.daily_pre_deal_shipped_units,
        p.daily_pre_deal_revenue,
        p.daily_pre_deal_total_customers_brand,
        p.daily_pre_deal_new_customers_brand,
        -- sns
        s.avg_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers,

        -- Calculate Delta Metrics
        (d.daily_deal_shipped_units - p.daily_pre_deal_shipped_units) as delta_daily_shipped_units_brand,
        (d.daily_deal_ops - p.daily_pre_deal_revenue) as delta_daily_revenue_brand,
        (d.daily_deal_total_customers_brand - p.daily_pre_deal_total_customers_brand) as delta_daily_customers_brand,
        (d.daily_deal_new_customers_brand - p.daily_pre_deal_new_customers_brand) as delta_daily_new_customers_brand,
        (s.avg_deal_sns_subscribers - s.avg_pre_deal_sns_subscribers) as delta_daily_sns_subscribers_brand

    FROM deal_metrics_brand d
        LEFT JOIN pre_deal_metrics_brand p
            ON d.brand_code = p.brand_code
            AND d.event_name = p.event_name
            AND d.event_year = p.event_year 
        LEFT JOIN (
            SELECT 
                brand_code,
                event_name,
                event_year,
                AVG(avg_deal_sns_subscribers) as avg_deal_sns_subscribers,
                AVG(avg_pre_deal_sns_subscribers) as avg_pre_deal_sns_subscribers
            FROM sns_metrics
            GROUP BY brand_code, event_name, event_year
        ) s
            ON d.brand_code = s.brand_code
            AND d.event_name = s.event_name
            AND d.event_year = s.event_year
    
    WHERE d.promo_start_date IS NOT NULL
    ORDER BY 
        d.promo_start_date DESC,
        d.daily_deal_ops * d.event_duration_days DESC
);

-- Calculate Last Year metrics - Brand Level
DROP TABLE IF EXISTS final_brand_metrics;
CREATE TEMP TABLE final_brand_metrics AS (
    SELECT
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
        t1.daily_deal_shipped_units as daily_deal_shipped_units_brand,
        t1.daily_deal_ops as daily_deal_revenue_brand,
        t1.daily_deal_total_customers_brand as daily_deal_customers_brand,
        t1.daily_deal_new_customers_brand,
        t1.daily_deal_return_customers_brand,
        t1.daily_deal_sns_subscribers as daily_deal_sns_subscribers_brand,

        -- Current Year Pre-Deal Period Metrics
        t1.daily_pre_deal_shipped_units as daily_pre_deal_shipped_units_brand,
        t1.daily_pre_deal_revenue as daily_pre_deal_revenue_brand,
        t1.daily_pre_deal_total_customers_brand as daily_pre_deal_customers_brand,
        t1.daily_pre_deal_new_customers_brand,
        t1.daily_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers_brand,

        -- Last Year Deal Period Metrics
        t2.daily_deal_shipped_units as ly_daily_deal_shipped_units_brand,
        t2.daily_deal_ops as ly_daily_deal_revenue_brand,
        t2.daily_deal_total_customers_brand as ly_daily_deal_customers_brand,
        t2.daily_deal_new_customers_brand as ly_daily_deal_new_customers_brand,
        t2.daily_deal_return_customers_brand as ly_daily_deal_return_customers_brand,
        t2.daily_deal_sns_subscribers as ly_daily_deal_sns_subscribers_brand,

        -- Last Year Pre-Deal Period Metrics
        t2.daily_pre_deal_shipped_units as ly_daily_pre_deal_shipped_units_brand,
        t2.daily_pre_deal_revenue as ly_daily_pre_deal_revenue_brand,
        t2.daily_pre_deal_total_customers_brand as ly_daily_pre_deal_customers_brand,
        t2.daily_pre_deal_new_customers_brand as ly_daily_pre_deal_new_customers_brand,
        t2.daily_pre_deal_sns_subscribers as ly_daily_pre_deal_sns_subscribers_brand,

        -- Delta Metrics
        t1.delta_daily_shipped_units_brand,
        t1.delta_daily_revenue_brand,
        t1.delta_daily_customers_brand,
        t1.delta_daily_new_customers_brand,
        t1.delta_daily_sns_subscribers_brand

    FROM deal_growth_brand t1
        LEFT JOIN deal_growth_brand t2
        ON t1.brand_code = t2.brand_code
        AND t1.event_name = t2.event_name
        AND t1.event_year - 1 = t2.event_year
    WHERE t1.event_name IS NOT NULL
);

-- final output Brand level
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_brand_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_brand_level AS (
    SELECT 
        -- Base Brand info
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
        daily_pre_deal_sns_subscribers_brand,
        
        -- Delta Metrics
        delta_daily_shipped_units_brand,
        delta_daily_revenue_brand,
        delta_daily_customers_brand,
        delta_daily_new_customers_brand,
        delta_daily_sns_subscribers_brand,
        
        -- Last Year Metrics
        ly_daily_deal_shipped_units_brand,
        ly_daily_deal_revenue_brand,
        ly_daily_deal_customers_brand,
        ly_daily_deal_new_customers_brand,
        ly_daily_deal_return_customers_brand,
        ly_daily_deal_sns_subscribers_brand,
        ly_daily_pre_deal_shipped_units_brand,
        ly_daily_pre_deal_revenue_brand,
        ly_daily_pre_deal_customers_brand,
        ly_daily_pre_deal_new_customers_brand,
        ly_daily_pre_deal_sns_subscribers_brand
    FROM final_brand_metrics
);


/*************************
# 4. FINAL TABLE CREATION
-- Company Level 
*************************/

--Always grouping by company+GL combination
DROP TABLE IF EXISTS deal_metrics_company;
CREATE TEMP TABLE deal_metrics_company AS (
    SELECT 
        gl_product_group,
        gl_product_group_name,
        company_code,
        company_name,
        event_name,
        promo_start_date,
        promo_end_date,
        event_year,
        event_month,
        event_duration_days,
        SUM(shipped_units)/event_duration_days as daily_deal_shipped_units,
        SUM(revenue_share_amt)/event_duration_days as daily_deal_ops,
        COUNT(DISTINCT customer_id)/event_duration_days as daily_deal_total_customers_company,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/event_duration_days as daily_deal_new_customers_company,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/event_duration_days as daily_deal_return_customers_company
    FROM deal_daily_summary
    GROUP BY 
        gl_product_group,
        gl_product_group_name,
        company_code,
        company_name,
        event_name,
        promo_start_date,
        promo_end_date,
        event_year,
        event_month,
        event_duration_days
);

DROP TABLE IF EXISTS pre_deal_metrics_company;
CREATE TEMP TABLE pre_deal_metrics_company AS (
    SELECT 
        gl_product_group,
        gl_product_group_name,
        company_code,
        company_name,
        event_name,
        promo_start_date,
        promo_end_date,
        event_year,
        event_month,
        event_duration_days,
        SUM(shipped_units)/91 as daily_pre_deal_shipped_units,
        SUM(revenue_share_amt)/91 as daily_pre_deal_revenue,
        COUNT(DISTINCT customer_id)/91 as daily_pre_deal_total_customers_company,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers_company
    FROM pre_deal_daily_summary
    GROUP BY 
        gl_product_group,
        gl_product_group_name,
        company_code,
        company_name,
        event_name,
        promo_start_date,
        promo_end_date,
        event_year,
        event_month,
        event_duration_days
);

-- Compare deal vs pre deal periods (delta) - Company Level 
DROP TABLE IF EXISTS deal_growth_company;
CREATE TEMP TABLE deal_growth_company AS (
    SELECT 
        -- Company info (always including GL)
        d.gl_product_group,
        d.gl_product_group_name,
        d.company_code,
        d.company_name,
        -- event info
        d.event_name,
        d.promo_start_date,
        d.promo_end_date,
        d.event_month,
        d.event_year,
        d.event_duration_days,
        
        -- during deal daily avg (Company level)
        d.daily_deal_shipped_units,
        d.daily_deal_ops,    
        d.daily_deal_total_customers_company,
        d.daily_deal_new_customers_company,
        d.daily_deal_return_customers_company,
        -- sns
        s.avg_deal_sns_subscribers as daily_deal_sns_subscribers,

        -- pre deal daily avg (Company level)
        p.daily_pre_deal_shipped_units,
        p.daily_pre_deal_revenue,
        p.daily_pre_deal_total_customers_company,
        p.daily_pre_deal_new_customers_company,
        -- sns
        s.avg_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers,

        -- Calculate Delta Metrics
        (d.daily_deal_shipped_units - p.daily_pre_deal_shipped_units) as delta_daily_shipped_units_company,
        (d.daily_deal_ops - p.daily_pre_deal_revenue) as delta_daily_revenue_company,
        (d.daily_deal_total_customers_company - p.daily_pre_deal_total_customers_company) as delta_daily_customers_company,
        (d.daily_deal_new_customers_company - p.daily_pre_deal_new_customers_company) as delta_daily_new_customers_company,
        (s.avg_deal_sns_subscribers - s.avg_pre_deal_sns_subscribers) as delta_daily_sns_subscribers_company

    FROM deal_metrics_company d
        LEFT JOIN pre_deal_metrics_company p
            ON d.gl_product_group = p.gl_product_group
            AND d.company_code = p.company_code
            AND d.event_name = p.event_name
            AND d.event_year = p.event_year 
        LEFT JOIN (
            SELECT 
                gl_product_group,
                company_code,
                event_name,
                event_year,
                AVG(avg_deal_sns_subscribers) as avg_deal_sns_subscribers,
                AVG(avg_pre_deal_sns_subscribers) as avg_pre_deal_sns_subscribers
            FROM sns_metrics
            GROUP BY gl_product_group, company_code, event_name, event_year
        ) s
            ON d.gl_product_group = s.gl_product_group
            AND d.company_code = s.company_code
            AND d.event_name = s.event_name
            AND d.event_year = s.event_year
    WHERE d.promo_start_date IS NOT NULL
    ORDER BY 
        d.promo_start_date DESC,
        d.daily_deal_ops * d.event_duration_days DESC
);

-- Calculate Last Year metrics - Company Level
DROP TABLE IF EXISTS final_company_metrics;
CREATE TEMP TABLE final_company_metrics AS (
    SELECT
        t1.gl_product_group,
        t1.gl_product_group_name,
        t1.company_code,
        t1.company_name,
        t1.event_name,
        t1.event_year,
        t1.event_duration_days,

        -- Current Year Deal Period Metrics
        t1.daily_deal_shipped_units as daily_deal_shipped_units_company,
        t1.daily_deal_ops as daily_deal_revenue_company,
        t1.daily_deal_total_customers_company as daily_deal_customers_company,
        t1.daily_deal_new_customers_company,
        t1.daily_deal_return_customers_company,
        t1.daily_deal_sns_subscribers as daily_deal_sns_subscribers_company,

        -- Current Year Pre-Deal Period Metrics
        t1.daily_pre_deal_shipped_units as daily_pre_deal_shipped_units_company,
        t1.daily_pre_deal_revenue as daily_pre_deal_revenue_company,
        t1.daily_pre_deal_total_customers_company as daily_pre_deal_customers_company,
        t1.daily_pre_deal_new_customers_company,
        t1.daily_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers_company,

        -- Last Year Deal Period Metrics
        t2.daily_deal_shipped_units as ly_daily_deal_shipped_units_company,
        t2.daily_deal_ops as ly_daily_deal_revenue_company,
        t2.daily_deal_total_customers_company as ly_daily_deal_customers_company,
        t2.daily_deal_new_customers_company as ly_daily_deal_new_customers_company,
        t2.daily_deal_return_customers_company as ly_daily_deal_return_customers_company,
        t2.daily_deal_sns_subscribers as ly_daily_deal_sns_subscribers_company,

        -- Last Year Pre-Deal Period Metrics
        t2.daily_pre_deal_shipped_units as ly_daily_pre_deal_shipped_units_company,
        t2.daily_pre_deal_revenue as ly_daily_pre_deal_revenue_company,
        t2.daily_pre_deal_total_customers_company as ly_daily_pre_deal_customers_company,
        t2.daily_pre_deal_new_customers_company as ly_daily_pre_deal_new_customers_company,
        t2.daily_pre_deal_sns_subscribers as ly_daily_pre_deal_sns_subscribers_company,

        -- Delta Metrics
        t1.delta_daily_shipped_units_company,
        t1.delta_daily_revenue_company,
        t1.delta_daily_customers_company,
        t1.delta_daily_new_customers_company,
        t1.delta_daily_sns_subscribers_company

    FROM deal_growth_company t1
        LEFT JOIN deal_growth_company t2
        ON t1.gl_product_group = t2.gl_product_group
        AND t1.company_code = t2.company_code
        AND t1.event_name = t2.event_name
        AND t1.event_year - 1 = t2.event_year
    WHERE t1.event_name IS NOT NULL
);

DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_company_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_company_level AS (
    SELECT 
        -- Base Company info (including GL)
        gl_product_group,
        gl_product_group_name,
        company_code,
        company_name,
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
        daily_pre_deal_sns_subscribers_company,
        
        -- Delta Metrics
        delta_daily_shipped_units_company,
        delta_daily_revenue_company,
        delta_daily_customers_company,
        delta_daily_new_customers_company,
        delta_daily_sns_subscribers_company,
        
        -- Last Year Metrics
        ly_daily_deal_shipped_units_company,
        ly_daily_deal_revenue_company,
        ly_daily_deal_customers_company,
        ly_daily_deal_new_customers_company,
        ly_daily_deal_return_customers_company,
        ly_daily_deal_sns_subscribers_company,
        ly_daily_pre_deal_shipped_units_company,
        ly_daily_pre_deal_revenue_company,
        ly_daily_pre_deal_customers_company,
        ly_daily_pre_deal_new_customers_company,
        ly_daily_pre_deal_sns_subscribers_company
    FROM final_company_metrics
);


/*************************
# 3. FINAL TABLE CREATION
-- GL Level 
*************************/

DROP TABLE IF EXISTS deal_metrics_gl;
CREATE TEMP TABLE deal_metrics_gl AS (
    SELECT 
        gl_product_group,
        gl_product_group_name,
        event_name,
        promo_start_date,
        promo_end_date,
        event_year,
        event_month,
        event_duration_days,
        SUM(shipped_units)/event_duration_days as daily_deal_shipped_units,
        SUM(revenue_share_amt)/event_duration_days as daily_deal_ops,
        COUNT(DISTINCT customer_id)/event_duration_days as daily_deal_total_customers_gl,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/event_duration_days as daily_deal_new_customers_gl,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/event_duration_days as daily_deal_return_customers_gl
    FROM deal_daily_summary
    GROUP BY 
        gl_product_group,
        gl_product_group_name,
        event_name,
        promo_start_date,
        promo_end_date,
        event_year,
        event_month,
        event_duration_days
);

DROP TABLE IF EXISTS pre_deal_metrics_gl;
CREATE TEMP TABLE pre_deal_metrics_gl AS (
    SELECT 
        gl_product_group,
        gl_product_group_name,
        event_name,
        promo_start_date,
        promo_end_date,
        event_year,
        event_month,
        event_duration_days,
        SUM(shipped_units)/91 as daily_pre_deal_shipped_units,
        SUM(revenue_share_amt)/91 as daily_pre_deal_revenue,
        COUNT(DISTINCT customer_id)/91 as daily_pre_deal_total_customers_gl,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers_gl
    FROM pre_deal_daily_summary
    GROUP BY 
        gl_product_group,
        gl_product_group_name,
        event_name,
        promo_start_date,
        promo_end_date,
        event_year,
        event_month,
        event_duration_days
);

-- Compare deal vs pre deal periods (delta) - GL Level
DROP TABLE IF EXISTS deal_growth_gl;
CREATE TEMP TABLE deal_growth_gl AS (
    SELECT 
        -- GL info
        d.gl_product_group,
        d.gl_product_group_name,
        -- event info
        d.event_name,
        d.promo_start_date,
        d.promo_end_date,
        d.event_month,
        d.event_year,
        d.event_duration_days,
        
        -- during deal daily avg (GL level)
        d.daily_deal_shipped_units,
        d.daily_deal_ops,    
        d.daily_deal_total_customers_gl,
        d.daily_deal_new_customers_gl,
        d.daily_deal_return_customers_gl,
        -- sns
        s.avg_deal_sns_subscribers as daily_deal_sns_subscribers,

        -- pre deal daily avg (GL level)
        p.daily_pre_deal_shipped_units,
        p.daily_pre_deal_revenue,
        p.daily_pre_deal_total_customers_gl,
        p.daily_pre_deal_new_customers_gl,
        -- sns
        s.avg_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers,

        -- Calculate Delta Metrics
        (d.daily_deal_shipped_units - p.daily_pre_deal_shipped_units) as delta_daily_shipped_units_gl,
        (d.daily_deal_ops - p.daily_pre_deal_revenue) as delta_daily_revenue_gl,
        (d.daily_deal_total_customers_gl - p.daily_pre_deal_total_customers_gl) as delta_daily_customers_gl,
        (d.daily_deal_new_customers_gl - p.daily_pre_deal_new_customers_gl) as delta_daily_new_customers_gl,
        (s.avg_deal_sns_subscribers - s.avg_pre_deal_sns_subscribers) as delta_daily_sns_subscribers_gl

    FROM deal_metrics_gl d
        LEFT JOIN pre_deal_metrics_gl p
            ON d.gl_product_group = p.gl_product_group
            AND d.event_name = p.event_name
            AND d.event_year = p.event_year 
        LEFT JOIN (
            SELECT 
                gl_product_group,
                event_name,
                event_year,
                AVG(avg_deal_sns_subscribers) as avg_deal_sns_subscribers,
                AVG(avg_pre_deal_sns_subscribers) as avg_pre_deal_sns_subscribers
            FROM sns_metrics
            GROUP BY gl_product_group, event_name, event_year
        ) s
            ON d.gl_product_group = s.gl_product_group
            AND d.event_name = s.event_name
            AND d.event_year = s.event_year
    WHERE d.promo_start_date IS NOT NULL
    ORDER BY 
        d.promo_start_date DESC,
        d.daily_deal_ops * d.event_duration_days DESC
);

-- Calculate Last Year metrics - GL Level
DROP TABLE IF EXISTS final_gl_metrics;
CREATE TEMP TABLE final_gl_metrics AS (
    SELECT
        t1.gl_product_group,
        t1.gl_product_group_name,
        t1.event_name,
        t1.event_year,
        t1.event_duration_days,

        -- Current Year Deal Period Metrics
        t1.daily_deal_shipped_units as daily_deal_shipped_units_gl,
        t1.daily_deal_ops as daily_deal_revenue_gl,
        t1.daily_deal_total_customers_gl as daily_deal_customers_gl,
        t1.daily_deal_new_customers_gl,
        t1.daily_deal_return_customers_gl,
        t1.daily_deal_sns_subscribers as daily_deal_sns_subscribers_gl,

        -- Current Year Pre-Deal Period Metrics
        t1.daily_pre_deal_shipped_units as daily_pre_deal_shipped_units_gl,
        t1.daily_pre_deal_revenue as daily_pre_deal_revenue_gl,
        t1.daily_pre_deal_total_customers_gl as daily_pre_deal_customers_gl,
        t1.daily_pre_deal_new_customers_gl,
        t1.daily_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers_gl,

        -- Last Year Deal Period Metrics
        t2.daily_deal_shipped_units as ly_daily_deal_shipped_units_gl,
        t2.daily_deal_ops as ly_daily_deal_revenue_gl,
        t2.daily_deal_total_customers_gl as ly_daily_deal_customers_gl,
        t2.daily_deal_new_customers_gl as ly_daily_deal_new_customers_gl,
        t2.daily_deal_return_customers_gl as ly_daily_deal_return_customers_gl,
        t2.daily_deal_sns_subscribers as ly_daily_deal_sns_subscribers_gl,

        -- Last Year Pre-Deal Period Metrics
        t2.daily_pre_deal_shipped_units as ly_daily_pre_deal_shipped_units_gl,
        t2.daily_pre_deal_revenue as ly_daily_pre_deal_revenue_gl,
        t2.daily_pre_deal_total_customers_gl as ly_daily_pre_deal_customers_gl,
        t2.daily_pre_deal_new_customers_gl as ly_daily_pre_deal_new_customers_gl,
        t2.daily_pre_deal_sns_subscribers as ly_daily_pre_deal_sns_subscribers_gl,

        -- Delta Metrics
        t1.delta_daily_shipped_units_gl,
        t1.delta_daily_revenue_gl,
        t1.delta_daily_customers_gl,
        t1.delta_daily_new_customers_gl,
        t1.delta_daily_sns_subscribers_gl

    FROM deal_growth_gl t1
        LEFT JOIN deal_growth_gl t2
        ON t1.gl_product_group = t2.gl_product_group
        AND t1.event_name = t2.event_name
        AND t1.event_year - 1 = t2.event_year
    WHERE t1.event_name IS NOT NULL
);

DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_gl_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_gl_level AS (
    SELECT 
        -- Base GL info
        gl_product_group,
        gl_product_group_name,
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
        
        -- Pre-Deal Period Metrics
        daily_pre_deal_shipped_units_gl,
        daily_pre_deal_revenue_gl,
        daily_pre_deal_customers_gl,
        daily_pre_deal_new_customers_gl,
        daily_pre_deal_sns_subscribers_gl,
        
        -- Delta Metrics
        delta_daily_shipped_units_gl,
        delta_daily_revenue_gl,
        delta_daily_customers_gl,
        delta_daily_new_customers_gl,
        delta_daily_sns_subscribers_gl,
        
        -- Last Year Metrics
        ly_daily_deal_shipped_units_gl,
        ly_daily_deal_revenue_gl,
        ly_daily_deal_customers_gl,
        ly_daily_deal_new_customers_gl,
        ly_daily_deal_return_customers_gl,
        ly_daily_deal_sns_subscribers_gl,
        ly_daily_pre_deal_shipped_units_gl,
        ly_daily_pre_deal_revenue_gl,
        ly_daily_pre_deal_customers_gl,
        ly_daily_pre_deal_new_customers_gl,
        ly_daily_pre_deal_sns_subscribers_gl
    FROM final_gl_metrics
);


/*************************
# 5. Final table creation
-- Event Level 
*************************/

DROP TABLE IF EXISTS deal_metrics_event;
CREATE TEMP TABLE deal_metrics_event AS (
    SELECT 
        event_name,
        promo_start_date,
        promo_end_date,
        event_year,
        event_month,
        event_duration_days,
        SUM(shipped_units)/event_duration_days as daily_deal_shipped_units,
        SUM(revenue_share_amt)/event_duration_days as daily_deal_ops,
        COUNT(DISTINCT customer_id)/event_duration_days as daily_deal_total_customers_event,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/event_duration_days as daily_deal_new_customers_event,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/event_duration_days as daily_deal_return_customers_event
    FROM deal_daily_summary
    GROUP BY 
        event_name,
        promo_start_date,
        promo_end_date,
        event_year,
        event_month,
        event_duration_days
);

DROP TABLE IF EXISTS pre_deal_metrics_event;
CREATE TEMP TABLE pre_deal_metrics_event AS (
    SELECT 
        event_name,
        promo_start_date,
        promo_end_date,
        event_year,
        event_month,
        event_duration_days,
        SUM(shipped_units)/91 as daily_pre_deal_shipped_units,
        SUM(revenue_share_amt)/91 as daily_pre_deal_revenue,
        COUNT(DISTINCT customer_id)/91 as daily_pre_deal_total_customers_event,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers_event
    FROM pre_deal_daily_summary
    GROUP BY 
        event_name,
        promo_start_date,
        promo_end_date,
        event_year,
        event_month,
        event_duration_days
);

-- Compare deal vs pre deal periods (delta) - Event Level
DROP TABLE IF EXISTS deal_growth_event;
CREATE TEMP TABLE deal_growth_event AS (
    SELECT 
        -- Event info
        d.event_name,
        d.promo_start_date,
        d.promo_end_date,
        d.event_month,
        d.event_year,
        d.event_duration_days,
        
        -- during deal daily avg (Event level)
        d.daily_deal_shipped_units,
        d.daily_deal_ops,    
        d.daily_deal_total_customers_event,
        d.daily_deal_new_customers_event,
        d.daily_deal_return_customers_event,
        -- sns
        s.avg_deal_sns_subscribers as daily_deal_sns_subscribers,

        -- pre deal daily avg (Event level)
        p.daily_pre_deal_shipped_units,
        p.daily_pre_deal_revenue,
        p.daily_pre_deal_total_customers_event,
        p.daily_pre_deal_new_customers_event,
        -- sns
        s.avg_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers,

        -- Calculate Delta Metrics
        (d.daily_deal_shipped_units - p.daily_pre_deal_shipped_units) as delta_daily_shipped_units_event,
        (d.daily_deal_ops - p.daily_pre_deal_revenue) as delta_daily_revenue_event,
        (d.daily_deal_total_customers_event - p.daily_pre_deal_total_customers_event) as delta_daily_customers_event,
        (d.daily_deal_new_customers_event - p.daily_pre_deal_new_customers_event) as delta_daily_new_customers_event,
        (s.avg_deal_sns_subscribers - s.avg_pre_deal_sns_subscribers) as delta_daily_sns_subscribers_event

    FROM deal_metrics_event d
        LEFT JOIN pre_deal_metrics_event p
            ON d.event_name = p.event_name
            AND d.event_year = p.event_year 
        LEFT JOIN (
            SELECT 
                event_name,
                event_year,
                AVG(avg_deal_sns_subscribers) as avg_deal_sns_subscribers,
                AVG(avg_pre_deal_sns_subscribers) as avg_pre_deal_sns_subscribers
            FROM sns_metrics
            GROUP BY event_name, event_year
        ) s
            ON d.event_name = s.event_name
            AND d.event_year = s.event_year
    WHERE d.promo_start_date IS NOT NULL
    ORDER BY 
        d.promo_start_date DESC,
        d.daily_deal_ops * d.event_duration_days DESC
);

-- Calculate Last Year metrics - Event Level
DROP TABLE IF EXISTS final_event_metrics;
CREATE TEMP TABLE final_event_metrics AS (
    SELECT
        t1.event_name,
        t1.event_year,
        t1.event_duration_days,

        -- Current Year Deal Period Metrics
        t1.daily_deal_shipped_units as daily_deal_shipped_units_event,
        t1.daily_deal_ops as daily_deal_revenue_event,
        t1.daily_deal_total_customers_event as daily_deal_customers_event,
        t1.daily_deal_new_customers_event,
        t1.daily_deal_return_customers_event,
        t1.daily_deal_sns_subscribers as daily_deal_sns_subscribers_event,

        -- Current Year Pre-Deal Period Metrics
        t1.daily_pre_deal_shipped_units as daily_pre_deal_shipped_units_event,
        t1.daily_pre_deal_revenue as daily_pre_deal_revenue_event,
        t1.daily_pre_deal_total_customers_event as daily_pre_deal_customers_event,
        t1.daily_pre_deal_new_customers_event,
        t1.daily_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers_event,

        -- Last Year Deal Period Metrics
        t2.daily_deal_shipped_units as ly_daily_deal_shipped_units_event,
        t2.daily_deal_ops as ly_daily_deal_revenue_event,
        t2.daily_deal_total_customers_event as ly_daily_deal_customers_event,
        t2.daily_deal_new_customers_event as ly_daily_deal_new_customers_event,
        t2.daily_deal_return_customers_event as ly_daily_deal_return_customers_event,
        t2.daily_deal_sns_subscribers as ly_daily_deal_sns_subscribers_event,

        -- Last Year Pre-Deal Period Metrics
        t2.daily_pre_deal_shipped_units as ly_daily_pre_deal_shipped_units_event,
        t2.daily_pre_deal_revenue as ly_daily_pre_deal_revenue_event,
        t2.daily_pre_deal_total_customers_event as ly_daily_pre_deal_customers_event,
        t2.daily_pre_deal_new_customers_event as ly_daily_pre_deal_new_customers_event,
        t2.daily_pre_deal_sns_subscribers as ly_daily_pre_deal_sns_subscribers_event,

        -- Delta Metrics
        t1.delta_daily_shipped_units_event,
        t1.delta_daily_revenue_event,
        t1.delta_daily_customers_event,
        t1.delta_daily_new_customers_event,
        t1.delta_daily_sns_subscribers_event

    FROM deal_growth_event t1
        LEFT JOIN deal_growth_event t2
        ON t1.event_name = t2.event_name
        AND t1.event_year - 1 = t2.event_year
    WHERE t1.event_name IS NOT NULL
);

-- final table creationEvent Level 
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_event_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_event_level AS (
    SELECT 
        -- Base Event info
        event_name,
        event_year,
        event_duration_days,
        
        -- Deal Period Metrics
        daily_deal_shipped_units_event,
        daily_deal_revenue_event,
        daily_deal_customers_event,
        daily_deal_new_customers_event,
        daily_deal_return_customers_event,
        daily_deal_sns_subscribers_event,
        
        -- Pre-Deal Period Metrics
        daily_pre_deal_shipped_units_event,
        daily_pre_deal_revenue_event,
        daily_pre_deal_customers_event,
        daily_pre_deal_new_customers_event,
        daily_pre_deal_sns_subscribers_event,
        
        -- Delta Metrics
        delta_daily_shipped_units_event,
        delta_daily_revenue_event,
        delta_daily_customers_event,
        delta_daily_new_customers_event,
        delta_daily_sns_subscribers_event,
        
        -- Last Year Metrics
        ly_daily_deal_shipped_units_event,
        ly_daily_deal_revenue_event,
        ly_daily_deal_customers_event,
        ly_daily_deal_new_customers_event,
        ly_daily_deal_return_customers_event,
        ly_daily_deal_sns_subscribers_event,
        ly_daily_pre_deal_shipped_units_event,
        ly_daily_pre_deal_revenue_event,
        ly_daily_pre_deal_customers_event,
        ly_daily_pre_deal_new_customers_event,
        ly_daily_pre_deal_sns_subscribers_event
    FROM final_event_metrics
);


-- Grant permissions for all tables
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_asin_level TO PUBLIC;
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_brand_level TO PUBLIC;
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_company_level TO PUBLIC;
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_gl_level TO PUBLIC;