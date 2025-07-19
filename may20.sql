--+ merchant_sku in booker.d_mp_merchant_sku_asin_map


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
        AND o.order_datetime BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '730 days' -- 2yr
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
                    AND TO_DATE(start_datetime, 'YYYY-MM-DD') BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '730 days'
                        AND TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
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

    WITH filtered_orders AS (
        -- Filter base_orders first to reduce data volume
        SELECT *
        FROM base_orders b
        WHERE EXISTS (
            SELECT 1 
            FROM pre_deal_date_ranges p 
            WHERE b.asin = p.asin
            AND b.order_date BETWEEN p.pre_deal_start_date AND p.pre_deal_end_date
        )
    )
    SELECT DISTINCT
        b.*,
        pdr.promo_start_date,
        pdr.promo_end_date,
        pdr.event_name,
        pdr.event_year,
        pdr.event_month,
        'PRE_DEAL' as period_type,
        'N' as is_promotion
    FROM filtered_orders b
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
            -- ASIN level metrics
            SUM(shipped_units) as shipped_units,
            SUM(revenue_share_amt) as revenue,
            SUM(display_ads_amt) as display_ads_amt,
            SUM(subscription_revenue_amt) as subscription_revenue_amt,
            COUNT(DISTINCT customer_id) as total_customers_asin,
            COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END) as new_customers_asin,
            COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END) as return_customers_asin
        FROM deal_daily_summary
        GROUP BY 1,2,3,4,5,6,7,8
    ),

    brand_metrics AS (
        SELECT 
            brand_code,
            event_name,
            promo_start_date,
            COUNT(DISTINCT customer_id) as total_customers_brand,
            COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END) as new_customers_brand,
            COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END) as return_customers_brand
        FROM deal_daily_summary
        GROUP BY 1,2,3
    )
    SELECT 
        bm.*,
        mam.dama_mfg_vendor_code as vendor_code,
        -- ASIN level daily metrics
        shipped_units/event_duration_days as daily_deal_shipped_units,
        revenue/event_duration_days as daily_deal_ops,            
        display_ads_amt/event_duration_days as daily_deal_display_ads_amt,
        subscription_revenue_amt/event_duration_days as daily_deal_subscription_revenue_amt,
        total_customers_asin/event_duration_days as daily_deal_total_customers_asin,
        new_customers_asin/event_duration_days as daily_deal_new_customers_asin,
        return_customers_asin/event_duration_days as daily_deal_return_customers_asin,
        -- Brand level daily metrics
        br.total_customers_brand/event_duration_days as daily_deal_total_customers_brand,
        br.new_customers_brand/event_duration_days as daily_deal_new_customers_brand,
        br.return_customers_brand/event_duration_days as daily_deal_return_customers_brand
    FROM base_metrics bm
        LEFT JOIN brand_metrics br
            ON bm.brand_code = br.brand_code 
            AND bm.event_name = br.event_name
            AND bm.promo_start_date = br.promo_start_date
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
    WITH asin_metrics AS (
        SELECT 
            asin,
            item_name,
            brand_code,
            event_name,
            promo_start_date,
            SUM(shipped_units)/91 as daily_pre_deal_shipped_units,  
            SUM(revenue_share_amt)/91 as daily_pre_deal_revenue,
            SUM(display_ads_amt)/91 as daily_pre_deal_display_ads_amt,
            SUM(subscription_revenue_amt)/91 as daily_pre_deal_subscription_revenue_amt,
            COUNT(DISTINCT customer_id)/91 as daily_pre_deal_total_customers_asin,  
            COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers_asin, 
            COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/91 as daily_pre_deal_return_customers_asin
        FROM pre_deal_daily_summary
        GROUP BY 1,2,3,4,5  -- Update grouping to include brand_code
    ),
    brand_metrics AS (
        SELECT 
            brand_code,
            event_name,
            promo_start_date,
            COUNT(DISTINCT customer_id)/91 as daily_pre_deal_total_customers_brand,  
            COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers_brand, 
            COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/91 as daily_pre_deal_return_customers_brand
        FROM pre_deal_daily_summary
        GROUP BY 1,2,3
    )
    SELECT 
        a.*,
        b.daily_pre_deal_total_customers_brand,
        b.daily_pre_deal_new_customers_brand,
        b.daily_pre_deal_return_customers_brand
    FROM asin_metrics a
    LEFT JOIN brand_metrics b
        ON a.brand_code = b.brand_code
        AND a.event_name = b.event_name 
        AND a.promo_start_date = b.promo_start_date
);


/*************************
SnS subscriber data
*************************/
DROP TABLE IF EXISTS sns_metrics;
CREATE TEMP TABLE sns_metrics AS (
    SELECT 
        sns.asin,
        p.promo_start_date,
        p.event_name,
        AVG(CASE 
            WHEN TO_DATE(snapshot_date, 'YYYY-MM-DD') BETWEEN promo_start_date AND promo_end_date 
            THEN active_subscription_count 
        END) as avg_deal_sns_subscribers,
        AVG(CASE 
            WHEN TO_DATE(snapshot_date, 'YYYY-MM-DD') BETWEEN promo_start_date - interval '91 day' AND promo_start_date - interval '1 day'
            THEN active_subscription_count 
        END) as avg_pre_deal_sns_subscribers
    FROM andes.subs_save_ddl.d_daily_active_sns_asin_detail sns
        INNER JOIN consolidated_promos p
        ON sns.asin = p.asin
    WHERE sns.marketplace_id = 7
        AND sns.gl_product_group in (510, 364, 325, 199, 194, 121, 75)
    GROUP BY 1,2,3
);


-- + booker.D_ASINS_MARKETPLACE_ATTRIBUTES.product_type (PL)
/*************************
Compare deal vs pre deal periods
*************************/
DROP TABLE IF EXISTS deal_growth;
CREATE TEMP TABLE deal_growth AS (
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
        
        -- during deal daily avg (ASIN level)
        d.daily_deal_shipped_units,
        d.daily_deal_ops,    
        d.daily_deal_display_ads_amt,
        d.daily_deal_subscription_revenue_amt,
        d.daily_deal_total_customers_asin,
        d.daily_deal_new_customers_asin,
        d.daily_deal_return_customers_asin,
        
        -- during deal daily avg (Brand level)
        d.daily_deal_total_customers_brand,
        d.daily_deal_new_customers_brand,
        d.daily_deal_return_customers_brand,
        
        s.avg_deal_sns_subscribers as daily_deal_sns_subscribers,

        -- pre deal daily avg (ASIN level)
        p.daily_pre_deal_shipped_units,
        p.daily_pre_deal_revenue,
        p.daily_pre_deal_display_ads_amt,
        p.daily_pre_deal_subscription_revenue_amt,
        p.daily_pre_deal_total_customers_asin,
        p.daily_pre_deal_new_customers_asin,
        p.daily_pre_deal_return_customers_asin,
        
        -- pre deal daily avg (Brand level)
        p.daily_pre_deal_total_customers_brand,
        p.daily_pre_deal_new_customers_brand,
        p.daily_pre_deal_return_customers_brand,
        
        s.avg_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers,

        -- Growth calculations (comparing daily averages) - ASIN level
        ROUND(
            CASE 
                WHEN COALESCE(p.daily_pre_deal_new_customers_asin, 0) = 0 
                THEN  ((d.daily_deal_new_customers_asin::FLOAT / 0.000000001) - 1)
                ELSE ((d.daily_deal_new_customers_asin::FLOAT / p.daily_pre_deal_new_customers_asin) - 1) 
            END, 
            2
        ) as daily_new_customer_growth_pct_asin,
        
        ROUND(
            CASE 
                WHEN COALESCE(p.daily_pre_deal_return_customers_asin, 0) = 0 
                THEN ((d.daily_deal_return_customers_asin::FLOAT / 0.000000001) - 1)
                ELSE ((d.daily_deal_return_customers_asin::FLOAT / p.daily_pre_deal_return_customers_asin) - 1)
            END,
            2
        ) as daily_return_customer_growth_pct_asin,
        
        -- Growth calculations (comparing daily averages) - Brand level
        ROUND(
            CASE 
                WHEN COALESCE(p.daily_pre_deal_new_customers_brand, 0) = 0 
                THEN  ((d.daily_deal_new_customers_brand::FLOAT / 0.000000001) - 1)
                ELSE ((d.daily_deal_new_customers_brand::FLOAT / p.daily_pre_deal_new_customers_brand) - 1) 
            END, 
            2
        ) as daily_new_customer_growth_pct_brand,
        
        ROUND(
            CASE 
                WHEN COALESCE(p.daily_pre_deal_return_customers_brand, 0) = 0 
                THEN ((d.daily_deal_return_customers_brand::FLOAT / 0.000000001) - 1)
                ELSE ((d.daily_deal_return_customers_brand::FLOAT / p.daily_pre_deal_return_customers_brand) - 1)
            END,
            2
        ) as daily_return_customer_growth_pct_brand,
        
        ROUND(
            CASE 
                WHEN COALESCE(s.avg_pre_deal_sns_subscribers, 0) = 0 
                THEN ((s.avg_deal_sns_subscribers::FLOAT / 0.000000001) - 1)
                ELSE ((s.avg_deal_sns_subscribers::FLOAT / s.avg_pre_deal_sns_subscribers) - 1)
            END,
            2
        ) as daily_sns_subscribers_growth_pct

    FROM deal_metrics d
        LEFT JOIN pre_deal_metrics p
            ON d.asin = p.asin
            AND d.event_name = p.event_name
            AND d.promo_start_date = p.promo_start_date
        LEFT JOIN sns_metrics s
            ON d.asin = s.asin
            AND d.event_name = s.event_name
            AND d.promo_start_date = s.promo_start_date
        LEFT JOIN andes.roi_ml_ddl.VENDOR_COMPANY_CODES v
            ON v.vendor_code = d.vendor_code

    WHERE d.promo_start_date IS NOT NULL
    ORDER BY 
        d.promo_start_date DESC,
        d.daily_deal_ops * d.event_duration_days DESC
);


/*************************
Final table creation
+ YoY calculations
*************************/
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_asin;
CREATE TABLE pm_sandbox_aqxiao.ntb_asin AS (
    SELECT
        -- asin info
        t1.asin,
        t1.item_name,
        t1.gl_product_group_name,
        t1.vendor_code,
        t1.company_name,
        t1.company_code,
        t1.brand_code,
        t1.brand_name,
        t1.event_name,

        -- curr period
        t1.promo_start_date,
        t1.promo_end_date,
        t1.event_month,
        t1.event_year,  
        t1.event_duration_days,

        -- deal (ASIN level)
        t1.daily_deal_shipped_units,
        t1.daily_deal_ops,  
        t1.daily_deal_display_ads_amt,
        t1.daily_deal_subscription_revenue_amt,
        t1.daily_deal_total_customers_asin,
        t1.daily_deal_new_customers_asin,    
        t1.daily_deal_return_customers_asin,
        
        -- deal (Brand level)
        t1.daily_deal_total_customers_brand,
        t1.daily_deal_new_customers_brand,
        t1.daily_deal_return_customers_brand,
        
        t1.daily_deal_sns_subscribers,

        -- pre deal (ASIN level)
        t1.daily_pre_deal_shipped_units,
        t1.daily_pre_deal_revenue,
        t1.daily_pre_deal_display_ads_amt,
        t1.daily_pre_deal_subscription_revenue_amt,
        t1.daily_pre_deal_total_customers_asin,
        t1.daily_pre_deal_new_customers_asin,    
        t1.daily_pre_deal_return_customers_asin,
        
        -- pre deal (Brand level)
        t1.daily_pre_deal_total_customers_brand,
        t1.daily_pre_deal_new_customers_brand,
        t1.daily_pre_deal_return_customers_brand,
        
        t1.daily_pre_deal_sns_subscribers,
        
        -- Growth percentages
        t1.daily_new_customer_growth_pct_asin,
        t1.daily_return_customer_growth_pct_asin,
        t1.daily_new_customer_growth_pct_brand,
        t1.daily_return_customer_growth_pct_brand,
        t1.daily_sns_subscribers_growth_pct,
        
        -- Last year's deal metrics (ASIN level)
        t2.daily_deal_shipped_units as ly_daily_deal_shipped_units,
        t2.daily_deal_ops as ly_daily_deal_ops,
        t2.daily_deal_display_ads_amt as ly_daily_deal_display_ads_amt,
        t2.daily_deal_subscription_revenue_amt as ly_daily_deal_subscription_revenue_amt,
        t2.daily_deal_total_customers_asin as ly_daily_deal_total_customers_asin,
        t2.daily_deal_new_customers_asin as ly_daily_deal_new_customers_asin,
        t2.daily_deal_return_customers_asin as ly_daily_deal_return_customers_asin,
        
        -- Last year's deal metrics (Brand level)
        t2.daily_deal_total_customers_brand as ly_daily_deal_total_customers_brand,
        t2.daily_deal_new_customers_brand as ly_daily_deal_new_customers_brand,
        t2.daily_deal_return_customers_brand as ly_daily_deal_return_customers_brand,
        
        t2.daily_deal_sns_subscribers as ly_daily_deal_sns_subscribers,

        -- Last year's pre-deal metrics (ASIN level)
        t2.daily_pre_deal_shipped_units as ly_daily_pre_deal_shipped_units,
        t2.daily_pre_deal_revenue as ly_daily_pre_deal_revenue,
        t2.daily_pre_deal_display_ads_amt as ly_daily_pre_deal_display_ads_amt,
        t2.daily_pre_deal_subscription_revenue_amt as ly_daily_pre_deal_subscription_revenue_amt,
        t2.daily_pre_deal_total_customers_asin as ly_daily_pre_deal_total_customers_asin,
        t2.daily_pre_deal_new_customers_asin as ly_daily_pre_deal_new_customers_asin,
        t2.daily_pre_deal_return_customers_asin as ly_daily_pre_deal_return_customers_asin,
        
        -- Last year's pre-deal metrics (Brand level)
        t2.daily_pre_deal_total_customers_brand as ly_daily_pre_deal_total_customers_brand,
        t2.daily_pre_deal_new_customers_brand as ly_daily_pre_deal_new_customers_brand,
        t2.daily_pre_deal_return_customers_brand as ly_daily_pre_deal_return_customers_brand,
        
        t2.daily_pre_deal_sns_subscribers as ly_daily_pre_deal_sns_subscribers

    FROM deal_growth t1
        LEFT JOIN deal_growth t2
        ON t1.asin = t2.asin
        AND t1.event_name = t2.event_name
        AND t1.event_year - 1 = t2.event_year
        
);


-- --Grant permissions
-- GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_asin TO PUBLIC;
