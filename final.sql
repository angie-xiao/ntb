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
FIRST PURCHASE - 
Single source of truth
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
        o.subscription_revenue_amt
    FROM deal_orders o
        LEFT JOIN first_purchases fp
        ON o.customer_id = fp.customer_id
        AND o.brand_code = fp.brand_code
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
        o.brand_code,
        o.brand_name,
        o.event_name,
        o.promo_start_date,
        o.promo_end_date,
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
        o.subscription_revenue_amt
    FROM pre_deal_orders o
        LEFT JOIN first_purchases fp
        ON o.customer_id = fp.customer_id
        AND o.brand_code = fp.brand_code
);


/*************************
Base metrics aggregation
*************************/
DROP TABLE IF EXISTS base_metrics;
CREATE TEMP TABLE base_metrics AS (
    SELECT 
        asin,
        item_name,
        gl_product_group,
        brand_code,
        brand_name,
        event_name,
        promo_start_date,
        promo_end_date,
        event_month,
        event_year,
        event_duration_days,
        SUM(shipped_units) as shipped_units,
        SUM(revenue_share_amt) as revenue,
        SUM(subscription_revenue_amt) as subscription_revenue_amt,
        COUNT(DISTINCT customer_id) as total_customers_asin,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END) as new_customers_asin,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END) as return_customers_asin
    FROM deal_daily_summary
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11 
);
    

-- 1. ASIN level metrics
DROP TABLE IF EXISTS asin_metrics;
CREATE TEMP TABLE asin_metrics AS (
    SELECT 
        asin,
        item_name,
        gl_product_group,
        brand_code, 
        brand_name,
        event_name,
        promo_start_date,
        event_year,
        event_month,  -- Add this line
        SUM(shipped_units)/91 as daily_pre_deal_shipped_units,
        SUM(revenue_share_amt)/91 as daily_pre_deal_revenue,
        SUM(subscription_revenue_amt)/91 as daily_pre_deal_subscription_revenue_amt,
        COUNT(DISTINCT customer_id)/91 as daily_pre_deal_total_customers_asin,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers_asin,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/91 as daily_pre_deal_return_customers_asin
    FROM pre_deal_daily_summary
    GROUP BY 1,2,3,4,5,6,7,8,9  -- Update the GROUP BY to include event_month
);


-- 2. Brand level metrics
DROP TABLE IF EXISTS brand_metrics;
CREATE TEMP TABLE brand_metrics AS (
    SELECT 
        brand_code,
        event_name,
        promo_start_date,
        event_year,
        event_month,
        -- Deal period metrics
        COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END) as total_customers_brand,
        COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END) as new_customers_brand,
        COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END) as return_customers_brand,
        
        -- Pre-deal period metrics
        COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END)/91 as daily_pre_deal_total_customers_brand,
        COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers_brand,
        COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/91 as daily_pre_deal_return_customers_brand
    FROM (
        SELECT 
            customer_id, 
            brand_code, 
            event_name,
            promo_start_date,
            event_year,
            event_month, 
            period_type,
            is_first_brand_purchase
        FROM deal_daily_summary
        UNION ALL
        SELECT 
            customer_id, 
            brand_code,
            event_name,
            promo_start_date,
            event_year,
            event_month,
            period_type,
            is_first_brand_purchase
        FROM pre_deal_daily_summary
    ) combined_data
    WHERE brand_code IS NOT NULL
    GROUP BY 1,2,3,4,5  -- Update GROUP BY to include event_month
);


-- 3. Company level metrics for deal period
DROP TABLE IF EXISTS company_deal_metrics;
CREATE TEMP TABLE company_deal_metrics AS (
    SELECT 
        d.gl_product_group, 
        v.company_code,
        d.event_name,
        d.promo_start_date,
        d.event_year,
        d.event_month,
        SUM(d.shipped_units) as shipped_units,
        SUM(d.revenue_share_amt) as revenue,
        SUM(d.subscription_revenue_amt) as subscription_revenue_amt,
        COUNT(DISTINCT d.customer_id) as total_customers_company,
        COUNT(DISTINCT CASE WHEN d.is_first_brand_purchase = 1 THEN d.customer_id END) as new_customers_company,
        COUNT(DISTINCT CASE WHEN d.is_first_brand_purchase = 0 THEN d.customer_id END) as return_customers_company
    FROM deal_daily_summary d
        LEFT JOIN andes.BOOKER.D_MP_ASIN_MANUFACTURER mam 
            ON mam.asin = d.asin 
            AND mam.marketplace_id = 7 
            AND mam.region_id = 1
        LEFT JOIN andes.roi_ml_ddl.vendor_company_codes v 
            ON mam.dama_mfg_vendor_code = v.vendor_code
    WHERE v.company_code IS NOT NULL
    GROUP BY 1,2,3,4,5,6
);


--4. Product Group metrics for deal period
DROP TABLE IF EXISTS gl_deal_metrics;
CREATE TEMP TABLE gl_deal_metrics AS (
    SELECT 
        gl_product_group,
        event_name,
        promo_start_date,
        event_year,
        event_month,
        SUM(shipped_units) as shipped_units,
        SUM(revenue_share_amt) as revenue,
        SUM(subscription_revenue_amt) as subscription_revenue_amt,
        COUNT(DISTINCT customer_id) as total_customers_gl,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END) as new_customers_gl,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END) as return_customers_gl
    FROM deal_daily_summary
    WHERE gl_product_group IS NOT NULL
    GROUP BY 1,2,3,4,5  -- Update GROUP BY to include event_month
);

-- 5. Company level metrics for pre-deal period
DROP TABLE IF EXISTS company_pre_deal_metrics;
CREATE TEMP TABLE company_pre_deal_metrics AS (
    SELECT 
        d.gl_product_group, 
        v.company_code,
        d.event_name,
        d.promo_start_date,
        d.event_year,
        d.event_month,  -- Add this line
        SUM(d.shipped_units)/91 as daily_pre_deal_shipped_units,
        SUM(d.revenue_share_amt)/91 as daily_pre_deal_revenue,
        SUM(d.subscription_revenue_amt)/91 as daily_pre_deal_subscription_revenue_amt,
        COUNT(DISTINCT d.customer_id)/91 as daily_pre_deal_total_customers_company,
        COUNT(DISTINCT CASE WHEN d.is_first_brand_purchase = 1 THEN d.customer_id END)/91 as daily_pre_deal_new_customers_company,
        COUNT(DISTINCT CASE WHEN d.is_first_brand_purchase = 0 THEN d.customer_id END)/91 as daily_pre_deal_return_customers_company
    FROM pre_deal_daily_summary d
        LEFT JOIN andes.BOOKER.D_MP_ASIN_MANUFACTURER mam 
            ON mam.asin = d.asin 
            AND mam.marketplace_id = 7 
            AND mam.region_id = 1
        LEFT JOIN andes.roi_ml_ddl.vendor_company_codes v 
            ON mam.dama_mfg_vendor_code = v.vendor_code
    WHERE v.company_code IS NOT NULL
    GROUP BY 1,2,3,4,5,6
);

-- 6. Product Group metrics for pre-deal period
DROP TABLE IF EXISTS gl_pre_deal_metrics;
CREATE TEMP TABLE gl_pre_deal_metrics AS (
    SELECT 
        gl_product_group,
        event_name,
        promo_start_date,
        event_year,
        event_month,
        SUM(shipped_units)/91 as daily_pre_deal_shipped_units,
        SUM(revenue_share_amt)/91 as daily_pre_deal_revenue,
        SUM(subscription_revenue_amt)/91 as daily_pre_deal_subscription_revenue_amt,
        COUNT(DISTINCT customer_id)/91 as daily_pre_deal_total_customers_gl,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers_gl,
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/91 as daily_pre_deal_return_customers_gl
    FROM pre_deal_daily_summary
    WHERE gl_product_group IS NOT NULL
    GROUP BY 1,2,3,4,5  -- Update GROUP BY to include event_month
);


-- Then modify deal_metrics to use these new tables:
DROP TABLE IF EXISTS deal_metrics;
CREATE TEMP TABLE deal_metrics AS (
    SELECT 
        bm.*,
        mam.dama_mfg_vendor_code as vendor_code,
        v.company_code,
        -- ASIN level daily metrics
        bm.shipped_units/event_duration_days as daily_deal_shipped_units,
        bm.revenue/event_duration_days as daily_deal_ops,            
        bm.subscription_revenue_amt/event_duration_days as daily_deal_subscription_revenue_amt,
        bm.total_customers_asin/event_duration_days as daily_deal_total_customers_asin,
        bm.new_customers_asin/event_duration_days as daily_deal_new_customers_asin,
        bm.return_customers_asin/event_duration_days as daily_deal_return_customers_asin,
        
        -- Brand level daily metrics
        br.total_customers_brand/event_duration_days as daily_deal_total_customers_brand,
        br.new_customers_brand/event_duration_days as daily_deal_new_customers_brand,
        br.return_customers_brand/event_duration_days as daily_deal_return_customers_brand,
        
        -- Company level daily metrics
        cm.total_customers_company/event_duration_days as daily_deal_total_customers_company,
        cm.new_customers_company/event_duration_days as daily_deal_new_customers_company,
        cm.return_customers_company/event_duration_days as daily_deal_return_customers_company,
        cm.shipped_units/event_duration_days as daily_deal_shipped_units_company,
        cm.revenue/event_duration_days as daily_deal_revenue_company,
        cm.subscription_revenue_amt/event_duration_days as daily_deal_subscription_revenue_company,
        
        -- GL level daily metrics
        gl.total_customers_gl/event_duration_days as daily_deal_total_customers_gl,
        gl.new_customers_gl/event_duration_days as daily_deal_new_customers_gl,
        gl.return_customers_gl/event_duration_days as daily_deal_return_customers_gl,
        gl.shipped_units/event_duration_days as daily_deal_shipped_units_gl,
        gl.revenue/event_duration_days as daily_deal_revenue_gl,
        gl.subscription_revenue_amt/event_duration_days as daily_deal_subscription_revenue_gl

    FROM base_metrics bm
        LEFT JOIN brand_metrics br
            ON bm.brand_code = br.brand_code 
            AND bm.event_name = br.event_name 
            AND bm.promo_start_date = br.promo_start_date
        LEFT JOIN andes.BOOKER.D_MP_ASIN_MANUFACTURER mam 
            ON mam.asin = bm.asin 
            AND mam.marketplace_id = 7 
            AND mam.region_id = 1
        LEFT JOIN andes.roi_ml_ddl.vendor_company_codes v 
            ON mam.dama_mfg_vendor_code = v.vendor_code
        LEFT JOIN company_deal_metrics cm 
            ON v.company_code = cm.company_code 
            AND bm.event_name = cm.event_name 
            AND bm.promo_start_date = cm.promo_start_date
        LEFT JOIN gl_deal_metrics gl 
            ON bm.gl_product_group = gl.gl_product_group 
            AND bm.event_name = gl.event_name 
            AND bm.promo_start_date = gl.promo_start_date
);


-- Similarly modify pre_deal_metrics to use new tables:
DROP TABLE IF EXISTS pre_deal_metrics;
CREATE TEMP TABLE pre_deal_metrics AS (
    SELECT 
        a.*,
        b.daily_pre_deal_total_customers_brand,
        b.daily_pre_deal_new_customers_brand,
        b.daily_pre_deal_return_customers_brand,
        
        -- Company metrics
        c.daily_pre_deal_total_customers_company,
        c.daily_pre_deal_new_customers_company,
        c.daily_pre_deal_return_customers_company,
        c.daily_pre_deal_shipped_units as daily_pre_deal_shipped_units_company,
        c.daily_pre_deal_revenue as daily_pre_deal_revenue_company,
        c.daily_pre_deal_subscription_revenue_amt as daily_pre_deal_subscription_revenue_company,
        
        -- GL metrics
        g.daily_pre_deal_total_customers_gl,
        g.daily_pre_deal_new_customers_gl,
        g.daily_pre_deal_return_customers_gl,
        g.daily_pre_deal_shipped_units as daily_pre_deal_shipped_units_gl,
        g.daily_pre_deal_revenue as daily_pre_deal_revenue_gl,
        g.daily_pre_deal_subscription_revenue_amt as daily_pre_deal_subscription_revenue_gl
    
    FROM asin_metrics a
        LEFT JOIN brand_metrics b
            ON a.brand_code = b.brand_code 
            AND a.event_name = b.event_name 
            AND a.promo_start_date = b.promo_start_date
        LEFT JOIN andes.BOOKER.D_MP_ASIN_MANUFACTURER mam 
            ON mam.asin = a.asin 
            AND mam.marketplace_id = 7 
            AND mam.region_id = 1
        LEFT JOIN andes.roi_ml_ddl.vendor_company_codes v 
            ON mam.dama_mfg_vendor_code = v.vendor_code
        LEFT JOIN company_pre_deal_metrics c 
            ON v.company_code = c.company_code 
            AND a.event_name = c.event_name 
            AND a.promo_start_date = c.promo_start_date
        LEFT JOIN gl_pre_deal_metrics g 
            ON a.gl_product_group = g.gl_product_group 
            AND a.event_name = g.event_name 
            AND a.promo_start_date = g.promo_start_date
);


/*************************
1. Base SNS data with AVG per ASIN
*************************/
DROP TABLE IF EXISTS base_sns_avg;
CREATE TEMP TABLE base_sns_avg AS (
    SELECT 
        sns.asin,
        sns.gl_product_group,
        mam.dama_mfg_vendor_code as vendor_code,
        v.company_code,
        p.promo_start_date,
        p.promo_end_date,
        p.event_name,
        p.event_year,
        p.event_month,  -- Add this line
        AVG(CASE 
            WHEN TO_DATE(sns.snapshot_date, 'YYYY-MM-DD') BETWEEN p.promo_start_date AND p.promo_end_date 
            THEN sns.active_subscription_count 
        END) as avg_deal_sns_subscribers,
        AVG(CASE 
            WHEN TO_DATE(sns.snapshot_date, 'YYYY-MM-DD') BETWEEN p.promo_start_date - interval '91 day' AND p.promo_start_date - interval '1 day'
            THEN sns.active_subscription_count 
        END) as avg_pre_deal_sns_subscribers
    FROM andes.subs_save_ddl.d_daily_active_sns_asin_detail sns
        INNER JOIN consolidated_promos p ON sns.asin = p.asin
        LEFT JOIN andes.BOOKER.D_MP_ASIN_MANUFACTURER mam 
            ON mam.asin = sns.asin 
            AND mam.marketplace_id = 7 
            AND mam.region_id = 1
        LEFT JOIN andes.roi_ml_ddl.vendor_company_codes v 
            ON mam.dama_mfg_vendor_code = v.vendor_code
    WHERE sns.marketplace_id = 7
        AND sns.gl_product_group in (510, 364, 325, 199, 194, 121, 75)
    GROUP BY 1,2,3,4,5,6,7,8,9
);


/*************************
2. Company level sums
*************************/
DROP TABLE IF EXISTS company_sns_sums;
CREATE TEMP TABLE company_sns_sums AS (
    SELECT 
        company_code,
        promo_start_date,
        event_name,
        event_year,
        event_month,  -- Add this line        
        SUM(avg_deal_sns_subscribers) as sum_deal_sns_subscribers_company,
        SUM(avg_pre_deal_sns_subscribers) as sum_pre_deal_sns_subscribers_company
    FROM base_sns_avg
    WHERE company_code IS NOT NULL
    GROUP BY 1,2,3,4,5
);


/*************************
3. Product Line level sums
*************************/
DROP TABLE IF EXISTS pl_sns_sums;
CREATE TEMP TABLE pl_sns_sums AS (
    SELECT 
        gl_product_group,
        promo_start_date,
        event_name,
        event_year,
        event_month,
        SUM(avg_deal_sns_subscribers) as sum_deal_sns_subscribers_pl,
        SUM(avg_pre_deal_sns_subscribers) as sum_pre_deal_sns_subscribers_pl
    FROM base_sns_avg
    WHERE gl_product_group IS NOT NULL
    GROUP BY 1,2,3,4,5
);


/*************************
4. Final SNS metrics combining all levels
*************************/
DROP TABLE IF EXISTS sns_metrics;
CREATE TEMP TABLE sns_metrics AS (
    SELECT 
        b.asin,
        b.gl_product_group,
        b.company_code,
        b.promo_start_date,
        b.promo_end_date,
        b.event_name,
        b.event_month,
        b.event_year,
        -- ASIN level
        b.avg_deal_sns_subscribers,
        b.avg_pre_deal_sns_subscribers,
        -- Company level
        c.sum_deal_sns_subscribers_company,
        c.sum_pre_deal_sns_subscribers_company,
        -- Product Line level
        p.sum_deal_sns_subscribers_pl,
        p.sum_pre_deal_sns_subscribers_pl
    FROM base_sns_avg b
        LEFT JOIN company_sns_sums c
            ON b.company_code = c.company_code
            AND b.promo_start_date = c.promo_start_date
            AND b.event_name = c.event_name
        LEFT JOIN pl_sns_sums p
            ON b.gl_product_group  = p.gl_product_group
            AND b.promo_start_date = p.promo_start_date
            AND b.event_name = p.event_name
);


/*************************
Compare deal vs pre deal periods
*************************/
DROP TABLE IF EXISTS deal_growth;
CREATE TEMP TABLE deal_growth AS (
    SELECT 
        -- Base ASIN info
        d.asin,
        d.item_name,
        (CASE 
            WHEN d.gl_product_group = 510 THEN 'Lux Beauty'
            WHEN d.gl_product_group = 364 THEN 'Personal Care Appliances'    
            WHEN d.gl_product_group = 325 THEN 'Grocery'
            WHEN d.gl_product_group = 199 THEN 'Pet'
            WHEN d.gl_product_group = 194 THEN 'Beauty'
            WHEN d.gl_product_group = 121 THEN 'HPC'
            WHEN d.gl_product_group = 75 THEN 'Baby'    
        END) as gl_product_group_name,
        d.gl_product_group,
        d.vendor_code,
        v.company_name,
        v.company_code, 
        d.brand_code,
        d.brand_name,
        d.event_name,

        -- Period info
        d.promo_start_date,
        d.promo_end_date,
        d.event_month,
        d.event_year,  
        d.event_duration_days,

        -- ASIN level metrics during deal
        d.daily_deal_shipped_units,
        d.daily_deal_ops,  
        d.daily_deal_subscription_revenue_amt,
        d.daily_deal_total_customers_asin,
        d.daily_deal_new_customers_asin,    
        d.daily_deal_return_customers_asin,
        s.avg_deal_sns_subscribers as daily_deal_sns_subscribers_asin,
        
        -- Brand level metrics during deal
        d.daily_deal_total_customers_brand,
        d.daily_deal_new_customers_brand,
        d.daily_deal_return_customers_brand,
        
        -- Company level metrics during deal
        d.daily_deal_total_customers_company,
        d.daily_deal_new_customers_company,
        d.daily_deal_return_customers_company,
        d.daily_deal_shipped_units_company,
        d.daily_deal_revenue_company,
        d.daily_deal_subscription_revenue_company,
        s.sum_deal_sns_subscribers_company as total_deal_sns_subscribers_company,
        s.sum_deal_sns_subscribers_company/d.event_duration_days as daily_deal_sns_subscribers_company,
        
        -- GL level metrics during deal
        d.daily_deal_total_customers_gl,
        d.daily_deal_new_customers_gl,
        d.daily_deal_return_customers_gl,
        d.daily_deal_shipped_units_gl,
        d.daily_deal_revenue_gl,
        d.daily_deal_subscription_revenue_gl,
        s.sum_deal_sns_subscribers_pl as total_deal_sns_subscribers_gl,
        s.sum_deal_sns_subscribers_pl/d.event_duration_days as daily_deal_sns_subscribers_gl,

        -- ASIN level metrics pre-deal
        p.daily_pre_deal_shipped_units,
        p.daily_pre_deal_revenue,
        p.daily_pre_deal_subscription_revenue_amt,
        p.daily_pre_deal_total_customers_asin,
        p.daily_pre_deal_new_customers_asin,    
        p.daily_pre_deal_return_customers_asin,
        s.avg_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers_asin,
        
        -- Brand level metrics pre-deal
        p.daily_pre_deal_total_customers_brand,
        p.daily_pre_deal_new_customers_brand,
        p.daily_pre_deal_return_customers_brand,
        
        -- Company level metrics pre-deal
        p.daily_pre_deal_total_customers_company,
        p.daily_pre_deal_new_customers_company,
        p.daily_pre_deal_return_customers_company,
        p.daily_pre_deal_shipped_units_company,
        p.daily_pre_deal_revenue_company,
        p.daily_pre_deal_subscription_revenue_company,
        s.sum_pre_deal_sns_subscribers_company as total_pre_deal_sns_subscribers_company,
        s.sum_pre_deal_sns_subscribers_company/91 as daily_pre_deal_sns_subscribers_company,
        
        -- GL level metrics pre-deal
        p.daily_pre_deal_total_customers_gl,
        p.daily_pre_deal_new_customers_gl,
        p.daily_pre_deal_return_customers_gl,
        p.daily_pre_deal_shipped_units_gl,
        p.daily_pre_deal_revenue_gl,
        p.daily_pre_deal_subscription_revenue_gl,
        s.sum_pre_deal_sns_subscribers_pl as total_pre_deal_sns_subscribers_gl,
        s.sum_pre_deal_sns_subscribers_pl/91 as daily_pre_deal_sns_subscribers_gl

    FROM deal_metrics d
        LEFT JOIN pre_deal_metrics p
            ON d.asin = p.asin
            AND d.event_name = p.event_name
            AND d.promo_start_date = p.promo_start_date
            AND d.event_month = p.event_month
            AND d.event_year = p.event_year
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
Intermediary table for final output
*************************/
DROP TABLE IF EXISTS combined_data;
CREATE TEMP TABLE combined_data AS (
    SELECT 
        dds.asin, 
        dds.item_name,
        dds.gl_product_group,
        dds.brand_code,
        dds.brand_name,
        dds.event_name,
        dds.promo_start_date,
        dds.promo_end_date,
        dds.event_month,
        dds.event_year,
        dds.event_duration_days,
        dds.period_type,
        dds.order_date,
        dds.customer_id,
        dds.is_first_brand_purchase,
        dds.shipped_units,
        dds.revenue_share_amt,
        dds.subscription_revenue_amt
    FROM deal_daily_summary dds
    
    UNION ALL
    
    SELECT 
        pds.asin, 
        pds.item_name,
        pds.gl_product_group,
        pds.brand_code,
        pds.brand_name,
        pds.event_name,
        pds.promo_start_date,
        pds.promo_end_date,
        pds.event_month,
        pds.event_year,
        91 as event_duration_days,
        pds.period_type,
        pds.order_date,
        pds.customer_id,
        pds.is_first_brand_purchase,
        pds.shipped_units,
        pds.revenue_share_amt,
        pds.subscription_revenue_amt
    FROM pre_deal_daily_summary pds
);



/*************************
Final table creation
*************************/
-- 1. ASIN Level Analysis
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_asin_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_asin_level AS (

    with cte as (
        SELECT 
            -- Base ASIN info
            d.asin,
            d.item_name,
            d.gl_product_group,
            (CASE 
                WHEN d.gl_product_group = 510 THEN 'Lux Beauty'
                WHEN d.gl_product_group = 364 THEN 'Personal Care Appliances'    
                WHEN d.gl_product_group = 325 THEN 'Grocery'
                WHEN d.gl_product_group = 199 THEN 'Pet'
                WHEN d.gl_product_group = 194 THEN 'Beauty'
                WHEN d.gl_product_group = 121 THEN 'HPC'
                WHEN d.gl_product_group = 75 THEN 'Baby'    
            END) as gl_product_group_name,
            d.brand_code,
            d.brand_name,
            d.event_name,
            d.event_year,
            d.event_duration_days,
            
            -- Deal Period Metrics
            SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END) as deal_shipped_units,
            SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END)/d.event_duration_days as daily_deal_shipped_units,
            SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END) as deal_revenue,
            SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END)/d.event_duration_days as daily_deal_revenue,
            SUM(CASE WHEN period_type = 'DEAL' THEN subscription_revenue_amt END) as deal_subscription_revenue,
            SUM(CASE WHEN period_type = 'DEAL' THEN subscription_revenue_amt END)/d.event_duration_days as daily_deal_subscription_revenue,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END) as deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END)/d.event_duration_days as daily_deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END) as deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/d.event_duration_days as daily_deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END) as deal_return_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/d.event_duration_days as daily_deal_return_customers,
            s.avg_deal_sns_subscribers as daily_deal_sns_subscribers,
            
            -- Pre-Deal Period Metrics
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN shipped_units END) as pre_deal_shipped_units,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN shipped_units END)/91 as daily_pre_deal_shipped_units,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN revenue_share_amt END) as pre_deal_revenue,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN revenue_share_amt END)/91 as daily_pre_deal_revenue,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN subscription_revenue_amt END) as pre_deal_subscription_revenue,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN subscription_revenue_amt END)/91 as daily_pre_deal_subscription_revenue,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END) as pre_deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END)/91 as daily_pre_deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END) as pre_deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END) as pre_deal_return_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/91 as daily_pre_deal_return_customers,
            
            -- sns
            s.avg_pre_deal_sns_subscribers as daily_pre_deal_sns_subscribers,

            -- Add delta calculations
            (SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END)/d.event_duration_days - 
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN shipped_units END)/91) as delta_daily_shipped_units,
            (SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END)/d.event_duration_days - 
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN revenue_share_amt END)/91) as delta_daily_revenue,
            (SUM(CASE WHEN period_type = 'DEAL' THEN subscription_revenue_amt END)/d.event_duration_days - 
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN subscription_revenue_amt END)/91) as delta_daily_subscription_revenue,
            (COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END)/d.event_duration_days - 
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END)/91) as delta_daily_customers,
            (COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/d.event_duration_days - 
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/91) as delta_daily_new_customers,
            (COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/d.event_duration_days - 
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/91) as delta_daily_return_customers,
            (s.avg_deal_sns_subscribers - s.avg_pre_deal_sns_subscribers) as delta_daily_sns_subscribers

        FROM combined_data d
            LEFT JOIN andes.BOOKER.D_MP_ASIN_MANUFACTURER mam 
                ON mam.asin = d.asin 
                AND mam.marketplace_id = 7 
                AND mam.region_id = 1
            LEFT JOIN andes.roi_ml_ddl.vendor_company_codes v 
                ON mam.dama_mfg_vendor_code = v.vendor_code
            LEFT JOIN sns_metrics s
                ON d.asin = s.asin
                AND d.event_name = s.event_name
                AND s.event_year = d.event_year

        GROUP BY 
            d.asin,
            d.item_name,
            d.gl_product_group,
            d.brand_code,
            d.brand_name,
            d.event_name,
            d.event_year,
            d.event_duration_days,
            s.avg_deal_sns_subscribers,
            s.avg_pre_deal_sns_subscribers
    )

    select 
        cte.*,
        -- Last year metrics
        -- deal
        ly.daily_deal_shipped_units as ly_daily_deal_shipped_units,
        ly.daily_deal_revenue as ly_daily_deal_revenue,
        ly.daily_deal_subscription_revenue as ly_daily_deal_subscription_revenue,
        ly.daily_deal_customers as ly_daily_deal_customers,
        ly.daily_deal_new_customers as ly_daily_deal_new_customers,
        ly.daily_deal_return_customers as ly_daily_deal_return_customers,
        -- pre deal
        ly.daily_pre_deal_shipped_units as ly_daily_pre_deal_shipped_units,
        ly.daily_pre_deal_revenue as ly_daily_pre_deal_revenue,
        ly.daily_pre_deal_subscription_revenue as ly_daily_pre_deal_subscription_revenue,
        ly.daily_pre_deal_customers as ly_daily_pre_deal_customers,
        ly.daily_pre_deal_new_customers as ly_daily_pre_deal_new_customers,
        ly.daily_pre_deal_return_customers as ly_daily_pre_deal_return_customers,
        -- sns
        s_ly.avg_deal_sns_subscribers as ly_daily_deal_sns_subscribers,
        s_ly.avg_pre_deal_sns_subscribers as ly_daily_pre_deal_sns_subscribers,
        -- Last year delta calculations
        ly.delta_daily_shipped_units as ly_delta_daily_shipped_units,
        ly.delta_daily_revenue as ly_delta_daily_revenue,
        ly.delta_daily_subscription_revenue as ly_delta_daily_subscription_revenue,
        ly.delta_daily_customers as ly_delta_daily_customers,
        ly.delta_daily_new_customers as ly_delta_daily_new_customers,
        ly.delta_daily_return_customers as ly_delta_daily_return_customers,
        (s_ly.avg_deal_sns_subscribers - s_ly.avg_pre_deal_sns_subscribers) as ly_delta_daily_sns_subscribers

    from cte
        LEFT JOIN sns_metrics s_ly
            ON cte.asin = s_ly.asin
            AND cte.event_name = s_ly.event_name
            AND cte.event_year - 1 = s_ly.event_year
        LEFT JOIN pm_sandbox_aqxiao.ntb_asin_level ly
            ON cte.asin = ly.asin
            AND cte.event_name = ly.event_name
            AND cte.event_year - 1 = ly.event_year
)
;

-- 2. Brand Level Analysis
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_brand_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_brand_level AS (
    with cte as (
        SELECT 
            -- Base Brand info
            d.brand_code,
            d.brand_name,
            d.event_name,
            d.event_year,
            d.event_duration_days,
            
            -- Deal Period Metrics
            SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END) as deal_shipped_units,
            SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END)/d.event_duration_days as daily_deal_shipped_units,
            SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END) as deal_revenue,
            SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END)/d.event_duration_days as daily_deal_revenue,
            SUM(CASE WHEN period_type = 'DEAL' THEN subscription_revenue_amt END) as deal_subscription_revenue,
            SUM(CASE WHEN period_type = 'DEAL' THEN subscription_revenue_amt END)/d.event_duration_days as daily_deal_subscription_revenue,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END) as deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END)/d.event_duration_days as daily_deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END) as deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/d.event_duration_days as daily_deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END) as deal_return_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/d.event_duration_days as daily_deal_return_customers,
            SUM(s.avg_deal_sns_subscribers) as daily_deal_sns_subscribers,
            
            -- Pre-Deal Period Metrics
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN shipped_units END) as pre_deal_shipped_units,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN shipped_units END)/91 as daily_pre_deal_shipped_units,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN revenue_share_amt END) as pre_deal_revenue,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN revenue_share_amt END)/91 as daily_pre_deal_revenue,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN subscription_revenue_amt END) as pre_deal_subscription_revenue,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN subscription_revenue_amt END)/91 as daily_pre_deal_subscription_revenue,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END) as pre_deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END)/91 as daily_pre_deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END) as pre_deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END) as pre_deal_return_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/91 as daily_pre_deal_return_customers,
            
            -- sns
            SUM(s.avg_pre_deal_sns_subscribers) as daily_pre_deal_sns_subscribers,

            -- Add delta calculations
            (SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END)/d.event_duration_days - 
             SUM(CASE WHEN period_type = 'PRE_DEAL' THEN shipped_units END)/91) as delta_daily_shipped_units,
            (SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END)/d.event_duration_days - 
             SUM(CASE WHEN period_type = 'PRE_DEAL' THEN revenue_share_amt END)/91) as delta_daily_revenue,
            (SUM(CASE WHEN period_type = 'DEAL' THEN subscription_revenue_amt END)/d.event_duration_days - 
             SUM(CASE WHEN period_type = 'PRE_DEAL' THEN subscription_revenue_amt END)/91) as delta_daily_subscription_revenue,
            (COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END)/d.event_duration_days - 
             COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END)/91) as delta_daily_customers,
            (COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/d.event_duration_days - 
             COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/91) as delta_daily_new_customers,
            (COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/d.event_duration_days - 
             COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/91) as delta_daily_return_customers,
            (SUM(s.avg_deal_sns_subscribers) - SUM(s.avg_pre_deal_sns_subscribers)) as delta_daily_sns_subscribers

        FROM combined_data d
            LEFT JOIN sns_metrics s
                ON d.asin = s.asin
                AND d.event_name = s.event_name
                AND s.event_year = d.event_year

        GROUP BY 
            d.brand_code,
            d.brand_name,
            d.event_name,
            d.event_year,
            d.event_duration_days
    )

    select 
        cte.*,
        -- Last year metrics
        -- deal
        ly.daily_deal_shipped_units as ly_daily_deal_shipped_units,
        ly.daily_deal_revenue as ly_daily_deal_revenue,
        ly.daily_deal_subscription_revenue as ly_daily_deal_subscription_revenue,
        ly.daily_deal_customers as ly_daily_deal_customers,
        ly.daily_deal_new_customers as ly_daily_deal_new_customers,
        ly.daily_deal_return_customers as ly_daily_deal_return_customers,
        -- pre deal
        ly.daily_pre_deal_shipped_units as ly_daily_pre_deal_shipped_units,
        ly.daily_pre_deal_revenue as ly_daily_pre_deal_revenue,
        ly.daily_pre_deal_subscription_revenue as ly_daily_pre_deal_subscription_revenue,
        ly.daily_pre_deal_customers as ly_daily_pre_deal_customers,
        ly.daily_pre_deal_new_customers as ly_daily_pre_deal_new_customers,
        ly.daily_pre_deal_return_customers as ly_daily_pre_deal_return_customers,
        -- sns
        s_ly.avg_deal_sns_subscribers as ly_daily_deal_sns_subscribers,
        s_ly.avg_pre_deal_sns_subscribers as ly_daily_pre_deal_sns_subscribers,
        -- Last year delta calculations
        ly.delta_daily_shipped_units as ly_delta_daily_shipped_units,
        ly.delta_daily_revenue as ly_delta_daily_revenue,
        ly.delta_daily_subscription_revenue as ly_delta_daily_subscription_revenue,
        ly.delta_daily_customers as ly_delta_daily_customers,
        ly.delta_daily_new_customers as ly_delta_daily_new_customers,
        ly.delta_daily_return_customers as ly_delta_daily_return_customers,
        (s_ly.avg_deal_sns_subscribers - s_ly.avg_pre_deal_sns_subscribers) as ly_delta_daily_sns_subscribers

    from cte
        LEFT JOIN sns_metrics s_ly
            ON cte.brand_code = s_ly.brand_code
            AND cte.event_name = s_ly.event_name
            AND cte.event_year - 1 = s_ly.event_year
        LEFT JOIN pm_sandbox_aqxiao.ntb_brand_level ly
            ON cte.brand_code = ly.brand_code
            AND cte.event_name = ly.event_name
            AND cte.event_year - 1 = ly.event_year
);

    


-- 3. Company Level Analysis
-- Same company is treated at different entities at different GLs
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_company_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_company_level AS (
    with cte as (
    SELECT
    -- Base Company info
    v.vendor_code as company_code,
    v.vendor_name as company_name,
    d.event_name,
    d.event_year,
    d.event_duration_days,

            -- Deal Period Metrics
            SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END) as deal_shipped_units,
            SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END)/d.event_duration_days as daily_deal_shipped_units,
            SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END) as deal_revenue,
            SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END)/d.event_duration_days as daily_deal_revenue,
            SUM(CASE WHEN period_type = 'DEAL' THEN subscription_revenue_amt END) as deal_subscription_revenue,
            SUM(CASE WHEN period_type = 'DEAL' THEN subscription_revenue_amt END)/d.event_duration_days as daily_deal_subscription_revenue,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END) as deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END)/d.event_duration_days as daily_deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END) as deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/d.event_duration_days as daily_deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END) as deal_return_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/d.event_duration_days as daily_deal_return_customers,
            SUM(s.avg_deal_sns_subscribers) as daily_deal_sns_subscribers,
            
            -- Pre-Deal Period Metrics
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN shipped_units END) as pre_deal_shipped_units,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN shipped_units END)/91 as daily_pre_deal_shipped_units,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN revenue_share_amt END) as pre_deal_revenue,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN revenue_share_amt END)/91 as daily_pre_deal_revenue,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN subscription_revenue_amt END) as pre_deal_subscription_revenue,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN subscription_revenue_amt END)/91 as daily_pre_deal_subscription_revenue,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END) as pre_deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END)/91 as daily_pre_deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END) as pre_deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END) as pre_deal_return_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/91 as daily_pre_deal_return_customers,
            
            -- sns
            SUM(s.avg_pre_deal_sns_subscribers) as daily_pre_deal_sns_subscribers,

            -- Add delta calculations
            (SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END)/d.event_duration_days - 
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN shipped_units END)/91) as delta_daily_shipped_units,
            (SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END)/d.event_duration_days - 
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN revenue_share_amt END)/91) as delta_daily_revenue,
            (SUM(CASE WHEN period_type = 'DEAL' THEN subscription_revenue_amt END)/d.event_duration_days - 
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN subscription_revenue_amt END)/91) as delta_daily_subscription_revenue,
            (COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END)/d.event_duration_days - 
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END)/91) as delta_daily_customers,
            (COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/d.event_duration_days - 
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/91) as delta_daily_new_customers,
            (COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/d.event_duration_days - 
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/91) as delta_daily_return_customers,
            (SUM(s.avg_deal_sns_subscribers) - SUM(s.avg_pre_deal_sns_subscribers)) as delta_daily_sns_subscribers

        FROM combined_data d
            LEFT JOIN andes.BOOKER.D_MP_ASIN_MANUFACTURER mam 
                ON mam.asin = d.asin 
                AND mam.marketplace_id = 7 
                AND mam.region_id = 1
            LEFT JOIN andes.roi_ml_ddl.vendor_company_codes v 
                ON mam.dama_mfg_vendor_code = v.vendor_code
            LEFT JOIN sns_metrics s
                ON d.asin = s.asin
                AND d.event_name = s.event_name
                AND s.event_year = d.event_year

        GROUP BY 
            v.vendor_code,
            v.vendor_name,
            d.event_name,
            d.event_year,
            d.event_duration_days
    )

    select 
        cte.*,
        -- Last year metrics
        -- deal
        ly.daily_deal_shipped_units as ly_daily_deal_shipped_units,
        ly.daily_deal_revenue as ly_daily_deal_revenue,
        ly.daily_deal_subscription_revenue as ly_daily_deal_subscription_revenue,
        ly.daily_deal_customers as ly_daily_deal_customers,
        ly.daily_deal_new_customers as ly_daily_deal_new_customers,
        ly.daily_deal_return_customers as ly_daily_deal_return_customers,
        -- pre deal
        ly.daily_pre_deal_shipped_units as ly_daily_pre_deal_shipped_units,
        ly.daily_pre_deal_revenue as ly_daily_pre_deal_revenue,
        ly.daily_pre_deal_subscription_revenue as ly_daily_pre_deal_subscription_revenue,
        ly.daily_pre_deal_customers as ly_daily_pre_deal_customers,
        ly.daily_pre_deal_new_customers as ly_daily_pre_deal_new_customers,
        ly.daily_pre_deal_return_customers as ly_daily_pre_deal_return_customers,
        -- sns
        s_ly.avg_deal_sns_subscribers as ly_daily_deal_sns_subscribers,
        s_ly.avg_pre_deal_sns_subscribers as ly_daily_pre_deal_sns_subscribers,
        -- Last year delta calculations
        ly.delta_daily_shipped_units as ly_delta_daily_shipped_units,
        ly.delta_daily_revenue as ly_delta_daily_revenue,
        ly.delta_daily_subscription_revenue as ly_delta_daily_subscription_revenue,
        ly.delta_daily_customers as ly_delta_daily_customers,
        ly.delta_daily_new_customers as ly_delta_daily_new_customers,
        ly.delta_daily_return_customers as ly_delta_daily_return_customers,
        (s_ly.avg_deal_sns_subscribers - s_ly.avg_pre_deal_sns_subscribers) as ly_delta_daily_sns_subscribers

    from cte
        LEFT JOIN sns_metrics s_ly
            ON cte.company_code = s_ly.company_code
            AND cte.event_name = s_ly.event_name
            AND cte.event_year - 1 = s_ly.event_year
        LEFT JOIN pm_sandbox_aqxiao.ntb_company_level ly
            ON cte.company_code = ly.company_code
            AND cte.event_name = ly.event_name
            AND cte.event_year - 1 = ly.event_year
);


-- 4. GL Level Analysis
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_gl_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_gl_level AS (
    with cte as (
    SELECT
    -- Base GL info
    d.gl_product_group,
    d.gl_product_group_name,
    d.event_name,
    d.event_year,
    d.event_duration_days,

            -- Deal Period Metrics
            SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END) as deal_shipped_units,
            SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END)/d.event_duration_days as daily_deal_shipped_units,
            SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END) as deal_revenue,
            SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END)/d.event_duration_days as daily_deal_revenue,
            SUM(CASE WHEN period_type = 'DEAL' THEN subscription_revenue_amt END) as deal_subscription_revenue,
            SUM(CASE WHEN period_type = 'DEAL' THEN subscription_revenue_amt END)/d.event_duration_days as daily_deal_subscription_revenue,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END) as deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END)/d.event_duration_days as daily_deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END) as deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/d.event_duration_days as daily_deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END) as deal_return_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/d.event_duration_days as daily_deal_return_customers,
            SUM(s.avg_deal_sns_subscribers) as daily_deal_sns_subscribers,
            
            -- Pre-Deal Period Metrics
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN shipped_units END) as pre_deal_shipped_units,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN shipped_units END)/91 as daily_pre_deal_shipped_units,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN revenue_share_amt END) as pre_deal_revenue,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN revenue_share_amt END)/91 as daily_pre_deal_revenue,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN subscription_revenue_amt END) as pre_deal_subscription_revenue,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN subscription_revenue_amt END)/91 as daily_pre_deal_subscription_revenue,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END) as pre_deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END)/91 as daily_pre_deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END) as pre_deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END) as pre_deal_return_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/91 as daily_pre_deal_return_customers,
            
            -- sns
            SUM(s.avg_pre_deal_sns_subscribers) as daily_pre_deal_sns_subscribers,

            -- Add delta calculations
            (SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END)/d.event_duration_days - 
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN shipped_units END)/91) as delta_daily_shipped_units,
            (SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END)/d.event_duration_days - 
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN revenue_share_amt END)/91) as delta_daily_revenue,
            (SUM(CASE WHEN period_type = 'DEAL' THEN subscription_revenue_amt END)/d.event_duration_days - 
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN subscription_revenue_amt END)/91) as delta_daily_subscription_revenue,
            (COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END)/d.event_duration_days - 
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END)/91) as delta_daily_customers,
            (COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/d.event_duration_days - 
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/91) as delta_daily_new_customers,
            (COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/d.event_duration_days - 
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/91) as delta_daily_return_customers,
            (SUM(s.avg_deal_sns_subscribers) - SUM(s.avg_pre_deal_sns_subscribers)) as delta_daily_sns_subscribers

        FROM combined_data d
            LEFT JOIN sns_metrics s
                ON d.asin = s.asin
                AND d.event_name = s.event_name
                AND s.event_year = d.event_year

        GROUP BY 
            d.gl_product_group,
            d.gl_product_group_name,
            d.event_name,
            d.event_year,
            d.event_duration_days
    )

    select 
        cte.*,
        -- Last year metrics
        -- deal
        ly.daily_deal_shipped_units as ly_daily_deal_shipped_units,
        ly.daily_deal_revenue as ly_daily_deal_revenue,
        ly.daily_deal_subscription_revenue as ly_daily_deal_subscription_revenue,
        ly.daily_deal_customers as ly_daily_deal_customers,
        ly.daily_deal_new_customers as ly_daily_deal_new_customers,
        ly.daily_deal_return_customers as ly_daily_deal_return_customers,
        -- pre deal
        ly.daily_pre_deal_shipped_units as ly_daily_pre_deal_shipped_units,
        ly.daily_pre_deal_revenue as ly_daily_pre_deal_revenue,
        ly.daily_pre_deal_subscription_revenue as ly_daily_pre_deal_subscription_revenue,
        ly.daily_pre_deal_customers as ly_daily_pre_deal_customers,
        ly.daily_pre_deal_new_customers as ly_daily_pre_deal_new_customers,
        ly.daily_pre_deal_return_customers as ly_daily_pre_deal_return_customers,
        -- sns
        s_ly.avg_deal_sns_subscribers as ly_daily_deal_sns_subscribers,
        s_ly.avg_pre_deal_sns_subscribers as ly_daily_pre_deal_sns_subscribers,
        -- Last year delta calculations
        ly.delta_daily_shipped_units as ly_delta_daily_shipped_units,
        ly.delta_daily_revenue as ly_delta_daily_revenue,
        ly.delta_daily_subscription_revenue as ly_delta_daily_subscription_revenue,
        ly.delta_daily_customers as ly_delta_daily_customers,
        ly.delta_daily_new_customers as ly_delta_daily_new_customers,
        ly.delta_daily_return_customers as ly_delta_daily_return_customers,
        (s_ly.avg_deal_sns_subscribers - s_ly.avg_pre_deal_sns_subscribers) as ly_delta_daily_sns_subscribers

    from cte
        LEFT JOIN sns_metrics s_ly
            ON cte.gl_product_group = s_ly.gl_product_group
            AND cte.event_name = s_ly.event_name
            AND cte.event_year - 1 = s_ly.event_year
        LEFT JOIN pm_sandbox_aqxiao.ntb_gl_level ly
            ON cte.gl_product_group = ly.gl_product_group
            AND cte.event_name = ly.event_name
            AND cte.event_year - 1 = ly.event_year
);


-- 5. Event Level Analysis
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_event_level;
CREATE TABLE pm_sandbox_aqxiao.ntb_event_level AS (
    with cte as (
        SELECT
            -- Base Event info
            d.event_name,
            d.event_year,
            d.event_duration_days,

            -- Deal Period Metrics
            SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END) as deal_shipped_units,
            SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END)/d.event_duration_days as daily_deal_shipped_units,
            SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END) as deal_revenue,
            SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END)/d.event_duration_days as daily_deal_revenue,
            SUM(CASE WHEN period_type = 'DEAL' THEN subscription_revenue_amt END) as deal_subscription_revenue,
            SUM(CASE WHEN period_type = 'DEAL' THEN subscription_revenue_amt END)/d.event_duration_days as daily_deal_subscription_revenue,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END) as deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END)/d.event_duration_days as daily_deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END) as deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/d.event_duration_days as daily_deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END) as deal_return_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/d.event_duration_days as daily_deal_return_customers,
            SUM(s.avg_deal_sns_subscribers) as daily_deal_sns_subscribers,
            
            -- Pre-Deal Period Metrics
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN shipped_units END) as pre_deal_shipped_units,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN shipped_units END)/91 as daily_pre_deal_shipped_units,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN revenue_share_amt END) as pre_deal_revenue,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN revenue_share_amt END)/91 as daily_pre_deal_revenue,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN subscription_revenue_amt END) as pre_deal_subscription_revenue,
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN subscription_revenue_amt END)/91 as daily_pre_deal_subscription_revenue,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END) as pre_deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END)/91 as daily_pre_deal_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END) as pre_deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/91 as daily_pre_deal_new_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END) as pre_deal_return_customers,
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/91 as daily_pre_deal_return_customers,
            
            -- sns
            SUM(s.avg_pre_deal_sns_subscribers) as daily_pre_deal_sns_subscribers,

            -- Add delta calculations
            (SUM(CASE WHEN period_type = 'DEAL' THEN shipped_units END)/d.event_duration_days - 
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN shipped_units END)/91) as delta_daily_shipped_units,
            (SUM(CASE WHEN period_type = 'DEAL' THEN revenue_share_amt END)/d.event_duration_days - 
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN revenue_share_amt END)/91) as delta_daily_revenue,
            (SUM(CASE WHEN period_type = 'DEAL' THEN subscription_revenue_amt END)/d.event_duration_days - 
            SUM(CASE WHEN period_type = 'PRE_DEAL' THEN subscription_revenue_amt END)/91) as delta_daily_subscription_revenue,
            (COUNT(DISTINCT CASE WHEN period_type = 'DEAL' THEN customer_id END)/d.event_duration_days - 
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' THEN customer_id END)/91) as delta_daily_customers,
            (COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/d.event_duration_days - 
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 1 THEN customer_id END)/91) as delta_daily_new_customers,
            (COUNT(DISTINCT CASE WHEN period_type = 'DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/d.event_duration_days - 
            COUNT(DISTINCT CASE WHEN period_type = 'PRE_DEAL' AND is_first_brand_purchase = 0 THEN customer_id END)/91) as delta_daily_return_customers,
            (SUM(s.avg_deal_sns_subscribers) - SUM(s.avg_pre_deal_sns_subscribers)) as delta_daily_sns_subscribers

        FROM combined_data d
            LEFT JOIN sns_metrics s
                ON d.asin = s.asin
                AND d.event_name = s.event_name
                AND s.event_year = d.event_year

        GROUP BY 
            d.event_name,
            d.event_year,
            d.event_duration_days
    )

    select 
        cte.*,
        -- Last year metrics
        -- deal
        ly.daily_deal_shipped_units as ly_daily_deal_shipped_units,
        ly.daily_deal_revenue as ly_daily_deal_revenue,
        ly.daily_deal_subscription_revenue as ly_daily_deal_subscription_revenue,
        ly.daily_deal_customers as ly_daily_deal_customers,
        ly.daily_deal_new_customers as ly_daily_deal_new_customers,
        ly.daily_deal_return_customers as ly_daily_deal_return_customers,
        -- pre deal
        ly.daily_pre_deal_shipped_units as ly_daily_pre_deal_shipped_units,
        ly.daily_pre_deal_revenue as ly_daily_pre_deal_revenue,
        ly.daily_pre_deal_subscription_revenue as ly_daily_pre_deal_subscription_revenue,
        ly.daily_pre_deal_customers as ly_daily_pre_deal_customers,
        ly.daily_pre_deal_new_customers as ly_daily_pre_deal_new_customers,
        ly.daily_pre_deal_return_customers as ly_daily_pre_deal_return_customers,
        -- sns
        s_ly.avg_deal_sns_subscribers as ly_daily_deal_sns_subscribers,
        s_ly.avg_pre_deal_sns_subscribers as ly_daily_pre_deal_sns_subscribers,
        -- Last year delta calculations
        ly.delta_daily_shipped_units as ly_delta_daily_shipped_units,
        ly.delta_daily_revenue as ly_delta_daily_revenue,
        ly.delta_daily_subscription_revenue as ly_delta_daily_subscription_revenue,
        ly.delta_daily_customers as ly_delta_daily_customers,
        ly.delta_daily_new_customers as ly_delta_daily_new_customers,
        ly.delta_daily_return_customers as ly_delta_daily_return_customers,
        (s_ly.avg_deal_sns_subscribers - s_ly.avg_pre_deal_sns_subscribers) as ly_delta_daily_sns_subscribers

    from cte
        LEFT JOIN sns_metrics s_ly
            ON cte.event_name = s_ly.event_name
            AND cte.event_year - 1 = s_ly.event_year
        LEFT JOIN pm_sandbox_aqxiao.ntb_event_level ly
            ON cte.event_name = ly.event_name
            AND cte.event_year - 1 = ly.event_year
);

-- Grant permissions for all tables
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_asin_level TO PUBLIC;
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_brand_level TO PUBLIC;
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_company_level TO PUBLIC;
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_gl_level TO PUBLIC;