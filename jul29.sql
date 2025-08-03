-- delta total/new custoomers for ly
--ops consistent

/*************************
ntoes on SnS Metrics:
-- for sns metrics beyond ASIN level
-- use count(distinct customer_id) 
    FROM SUBS_SAVE_REPORTING.FCT_SNS_SALES_DETAILS_DAILY 
-- once subscription request approved
*************************/

-------------------------------------- TRANSACTIONS --------------------------------------

/*************************
Base Orders Query
- Includes only retail merchant orders with shipped units > 0 
- Excludes cancelled or fraudulent orders
- Filters for last 730 days 
*************************/

DROP TABLE IF EXISTS base_orders;
CREATE TEMP TABLE base_orders AS (

    SELECT DISTINCT
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
            AND o.marketplace_id = cp.marketplace_id
            AND cp.marketplace_id = 7
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
        AND o.order_condition != 6
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
        AND TO_DATE(snapshot_date, 'YYYY-MM-DD') >= TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '730 days'
    GROUP BY 
        asin,
        TO_DATE(snapshot_date, 'YYYY-MM-DD')
);


---------------------------------------- DEALS ----------------------------------------

DROP TABLE IF EXISTS raw_events;
CREATE TEMP TABLE raw_events AS  (
    SELECT DISTINCT
        f.asin,
        f.customer_shipment_item_id,
        TO_DATE(p.start_datetime, 'YYYY-MM-DD') as promo_start_date,
        TO_DATE(p.end_datetime, 'YYYY-MM-DD') as promo_end_date,
        DATE_PART('year', p.start_datetime) as event_year,
        DATE_PART('month', p.start_datetime) as event_month,
        (CASE 
            WHEN p.promotion_key IS NULL THEN 'NO_PROMOTION'

            -- tier 1
            WHEN UPPER(p.promotion_internal_title) LIKE '%BSS%' 
                OR UPPER(p.promotion_internal_title) LIKE '%BIG SPRING SALE%' 
                THEN 'BSS'

            -- Prime Day logic with month boundary consideration
            WHEN (DATE_PART('month', p.start_datetime) = 7 
                OR (DATE_PART('month', p.start_datetime) = 6 
                    AND DATE_PART('day', p.start_datetime) >= 25))  -- Added buffer for late June starts
                AND (
                    UPPER(p.promotion_internal_title) LIKE '%PRIME%DAY%'
                    OR UPPER(p.promotion_internal_title) LIKE '%PD%' 
                    OR UPPER(p.promotion_internal_title) LIKE '%PEBD%' 
                )
                THEN 'PRIME DAY'

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
            -- Mother's Day with buffer
            WHEN (DATE_PART('month', p.start_datetime) = 5 
                OR (DATE_PART('month', p.start_datetime) = 4 
                    AND DATE_PART('day', p.start_datetime) >= 25))
                AND (UPPER(p.promotion_internal_title) LIKE '%MOTHER%DAY%' 
                    OR UPPER(p.promotion_internal_title) LIKE '%MOTHERS%DAY%' 
                    OR UPPER(p.promotion_internal_title) LIKE '%MOTHER_S%DAY%'
                    OR UPPER(p.promotion_internal_title) LIKE '%MOTHER''''S%DAY%')
                THEN 'MOTHERS DAY'
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
            -- Special handling for Pet Month/Day
            WHEN (DATE_PART('month', p.start_datetime) = 5 
                OR (DATE_PART('month', p.start_datetime) = 4 
                    AND DATE_PART('day', p.start_datetime) >= 25))  -- Consider late April starts as May
                AND (
                    UPPER(p.promotion_internal_title) LIKE '%PET%DAY%' 
                    OR UPPER(p.promotion_internal_title) LIKE '%PET%MONTH%'
                )
                THEN 'PET DAY'
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
    WHERE p.marketplace_key = 7
        AND p.approval_status IN ('Approved', 'Scheduled')
        AND p.promotion_type IN ('Best Deal', 'Deal of the Day', 'Lightning Deal', 'Event Deal')
        AND UPPER(p.promotion_internal_title) NOT LIKE '%OIH%'
        AND UPPER(p.promotion_internal_title) NOT LIKE '%LEAD%'
        AND TO_DATE(p.start_datetime, 'YYYY-MM-DD') 
            BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '730 days'
            AND TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
); 



DROP TABLE IF EXISTS raw_events;
CREATE TEMP TABLE raw_events AS  (
    SELECT DISTINCT
        f.asin,
        f.customer_shipment_item_id,
        TO_DATE(p.start_datetime, 'YYYY-MM-DD') as promo_start_date,
        TO_DATE(p.end_datetime, 'YYYY-MM-DD') as promo_end_date,
        DATE_PART('year', p.start_datetime) as event_year,
        DATE_PART('month', p.start_datetime) as event_month,
        (CASE 
            WHEN p.promotion_key IS NULL THEN 'NO_PROMOTION'

            -- tier 1
            WHEN UPPER(p.promotion_internal_title) LIKE '%BSS%' 
                OR UPPER(p.promotion_internal_title) LIKE '%BIG SPRING SALE%' 
                THEN 'BSS'

            -- Prime Day logic with month boundary consideration
            WHEN (DATE_PART('month', p.start_datetime) = 7 
                OR (DATE_PART('month', p.start_datetime) = 6 
                    AND DATE_PART('day', p.start_datetime) >= 25))  -- Added buffer for late June starts
                AND (
                    UPPER(p.promotion_internal_title) LIKE '%PRIME%DAY%'
                    OR UPPER(p.promotion_internal_title) LIKE '%PD%' 
                    OR UPPER(p.promotion_internal_title) LIKE '%PEBD%' 
                )
                THEN 'PRIME DAY'

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
            -- Mother's Day with buffer
            WHEN (DATE_PART('month', p.start_datetime) = 5 
                OR (DATE_PART('month', p.start_datetime) = 4 
                    AND DATE_PART('day', p.start_datetime) >= 25))
                AND (UPPER(p.promotion_internal_title) LIKE '%MOTHER%DAY%' 
                    OR UPPER(p.promotion_internal_title) LIKE '%MOTHERS%DAY%' 
                    OR UPPER(p.promotion_internal_title) LIKE '%MOTHER_S%DAY%'
                    OR UPPER(p.promotion_internal_title) LIKE '%MOTHER''''S%DAY%')
                THEN 'MOTHERS DAY'
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
            -- Special handling for Pet Month/Day
            WHEN (DATE_PART('month', p.start_datetime) = 5 
                OR (DATE_PART('month', p.start_datetime) = 4 
                    AND DATE_PART('day', p.start_datetime) >= 25))  -- Consider late April starts as May
                AND (
                    UPPER(p.promotion_internal_title) LIKE '%PET%DAY%' 
                    OR UPPER(p.promotion_internal_title) LIKE '%PET%MONTH%'
                )
                THEN 'PET DAY'
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
    WHERE p.marketplace_key = 7
        AND p.approval_status IN ('Approved', 'Scheduled')
        AND p.promotion_type IN ('Best Deal', 'Deal of the Day', 'Lightning Deal', 'Event Deal')
        AND UPPER(p.promotion_internal_title) NOT LIKE '%OIH%'
        AND UPPER(p.promotion_internal_title) NOT LIKE '%LEAD%'
        AND TO_DATE(p.start_datetime, 'YYYY-MM-DD') 
            BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '60 days'
            AND TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
); 


/*************************
Promotion Details
- Classifies promotions into major event types
https://w.amazon.com/bin/view/Canada_Marketing/Events/2025_Events/
*************************/
DROP TABLE IF EXISTS promotion_details;
CREATE TEMP TABLE promotion_details AS (

    WITH event_priority AS (
        SELECT 
            asin,
            customer_shipment_item_id,
            event_name,
            event_year,
            event_month,
            promo_start_date,
            promo_end_date,
            (CASE event_name
                WHEN 'PRIME DAY' THEN 1
                WHEN 'BLACK FRIDAY' THEN 2
                WHEN 'CYBER MONDAY' THEN 3
                WHEN 'BOXING WEEK' THEN 4
                WHEN 'BSS' THEN 5
                WHEN 'NYNY' THEN 6
                ELSE 99
            END) as event_priority_order
        FROM raw_events
    ),

    -- Handle overlaps by prioritizing events
    prioritized_events AS (
        SELECT 
            asin,
            customer_shipment_item_id,
            event_name,
            event_year,
            event_month,
            promo_start_date,
            promo_end_date,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    asin,
                    event_month
                ORDER BY 
                    event_priority_order,
                    promo_start_date
            ) as event_rank
        FROM event_priority
    )

    SELECT DISTINCT
        asin,
        customer_shipment_item_id,
        event_name,
        event_year,
        event_month,
        promo_start_date,
        promo_end_date
    FROM prioritized_events
    WHERE event_rank = 1  -- Only take highest priority event when overlapping
);


-- Find the most common start/end dates for each event
DROP TABLE IF EXISTS event_standards;
CREATE TEMP TABLE event_standards AS (
    WITH event_counts AS (
        SELECT 
            event_name,
            DATE_PART('year', promo_start_date) as event_year,
            promo_start_date,
            promo_end_date,
            COUNT(*) as frequency,
            -- Rank by frequency - removed month from partition
            ROW_NUMBER() OVER (
                PARTITION BY event_name, 
                DATE_PART('year', promo_start_date)
                ORDER BY COUNT(*) DESC
            ) as rn
        FROM promotion_details
        WHERE event_name != 'NO_PROMOTION'
        GROUP BY 
            event_name,
            DATE_PART('year', promo_start_date),
            promo_start_date,
            promo_end_date
    )
    SELECT 
        event_name,
        event_year,
        DATE_PART('month', promo_start_date) as event_month,  -- derived from the most common start date
        promo_start_date,
        promo_end_date,
        frequency
    FROM event_counts
    WHERE rn = 1
        AND frequency >= 3  -- Only keep patterns used by at least 3 promotions
);

-- Final consolidated promotions
DROP TABLE IF EXISTS consolidated_promos;
CREATE TEMP TABLE consolidated_promos AS (
    SELECT 
        p.asin,
        p.customer_shipment_item_id,
        p.event_name,
        DATE_PART('year', p.promo_start_date) as event_year,
        -- Use the standard event month for consistency
        COALESCE(e.event_month, DATE_PART('month', p.promo_start_date)) as event_month,
        -- Use standard dates if they exist, otherwise use original dates
        COALESCE(e.promo_start_date, p.promo_start_date) as promo_start_date,
        COALESCE(e.promo_end_date, p.promo_end_date) as promo_end_date
    FROM promotion_details p
        LEFT JOIN event_standards e
        ON p.event_name = e.event_name
        AND DATE_PART('year', p.promo_start_date) = e.event_year
    WHERE p.event_name != 'NO_PROMOTION'
);


/*************************
Deal Metrics - Base table with date ranges
*************************/
DROP TABLE IF EXISTS deal_base;
CREATE TEMP TABLE deal_base AS (
    SELECT DISTINCT
        customer_shipment_item_id,
        asin,
        event_name,
        event_year,
        promo_start_date,
        promo_end_date,
        event_month,
        -- Deal period
        promo_start_date as deal_start_date,
        promo_end_date as deal_end_date,
        -- Pre-deal period (T4W)
        promo_start_date - interval '29 day' as pre_deal_start_date,
        promo_start_date - interval '1 day' as pre_deal_end_date,
        -- Calculate event duration once
        (CASE 
            WHEN promo_end_date >= TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
                THEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - promo_start_date + 1
            ELSE promo_end_date - promo_start_date + 1
        END) AS event_duration_days
    FROM consolidated_promos
);


/*************************
Create unified base table combining orders and metrics
*************************/
DROP TABLE IF EXISTS unified_deal_base;
CREATE TEMP TABLE unified_deal_base AS (
    
    SELECT 
        -- Deal context
        d.customer_shipment_item_id,
        b.customer_id,
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
        b.shipped_units,
        b.revenue_share_amt,
        COALESCE(s.daily_sns_subscribers, 0) as daily_sns_subscribers

    FROM deal_base d
        INNER JOIN base_orders b 
            ON d.asin = b.asin
            AND b.order_date BETWEEN d.pre_deal_start_date AND d.deal_end_date
            AND b.customer_shipment_item_id = d.customer_shipment_item_id
        LEFT JOIN daily_sns_metrics s
            ON b.asin = s.asin
            AND b.order_date = s.metric_date
);


------------------------------------ FIRST PURCHASES ------------------------------------

/*************************
Combined First Purchases Calculation
*************************/

-- First get each customer's earliest purchase in last 365 days
DROP TABLE IF EXISTS customer_first_purchases;
CREATE TEMP TABLE customer_first_purchases AS (
    SELECT 
        customer_id,
        asin,
        brand_code,
        company_code,
        gl_product_group,
        MIN(order_date) OVER (PARTITION BY customer_id, asin) as first_asin_purchase,
        MIN(order_date) OVER (PARTITION BY customer_id, brand_code) as first_brand_purchase,
        MIN(order_date) OVER (PARTITION BY customer_id, company_code, gl_product_group) as first_company_purchase,
        MIN(order_date) OVER (PARTITION BY customer_id, gl_product_group) as first_gl_purchase
    FROM base_orders  -- This already has the 365 days filter from earlier
    WHERE order_date BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '365 days'
            AND TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
);


-- flag if first purchase for asin/brand/company/gl levels
DROP TABLE IF EXISTS unified_daily_metrics;
CREATE TEMP TABLE unified_daily_metrics AS (
    SELECT 
        udm.*,
        -- ASIN level: first purchase if no purchases before period AND purchase is within period
        CASE WHEN udm.period_type = 'DEAL' 
             AND (fp.first_asin_purchase IS NULL 
                  OR (fp.first_asin_purchase >= udm.deal_start_date 
                      AND fp.first_asin_purchase <= udm.deal_end_date))
             THEN 1 
             WHEN udm.period_type = 'PRE_DEAL'
             AND (fp.first_asin_purchase IS NULL 
                  OR (fp.first_asin_purchase >= udm.pre_deal_start_date 
                      AND fp.first_asin_purchase <= udm.pre_deal_end_date))
             THEN 1
             ELSE 0 
        END as is_first_asin_purchase,
        
        -- Brand level
        CASE WHEN udm.period_type = 'DEAL' 
             AND (fp.first_brand_purchase IS NULL 
                  OR (fp.first_brand_purchase >= udm.deal_start_date 
                      AND fp.first_brand_purchase <= udm.deal_end_date))
             THEN 1 
             WHEN udm.period_type = 'PRE_DEAL'
             AND (fp.first_brand_purchase IS NULL 
                  OR (fp.first_brand_purchase >= udm.pre_deal_start_date 
                      AND fp.first_brand_purchase <= udm.pre_deal_end_date))
             THEN 1
             ELSE 0 
        END as is_first_brand_purchase,
        
        -- Company level
        CASE WHEN udm.period_type = 'DEAL' 
             AND (fp.first_company_purchase IS NULL 
                  OR (fp.first_company_purchase >= udm.deal_start_date 
                      AND fp.first_company_purchase <= udm.deal_end_date))
             THEN 1 
             WHEN udm.period_type = 'PRE_DEAL'
             AND (fp.first_company_purchase IS NULL 
                  OR (fp.first_company_purchase >= udm.pre_deal_start_date 
                      AND fp.first_company_purchase <= udm.pre_deal_end_date))
             THEN 1
             ELSE 0 
        END as is_first_company_purchase,
        
        -- GL level
        CASE WHEN udm.period_type = 'DEAL' 
             AND (fp.first_gl_purchase IS NULL 
                  OR (fp.first_gl_purchase >= udm.deal_start_date 
                      AND fp.first_gl_purchase <= udm.deal_end_date))
             THEN 1 
             WHEN udm.period_type = 'PRE_DEAL'
             AND (fp.first_gl_purchase IS NULL 
                  OR (fp.first_gl_purchase >= udm.pre_deal_start_date 
                      AND fp.first_gl_purchase <= udm.pre_deal_end_date))
             THEN 1
             ELSE 0 
        END as is_first_gl_purchase
    
    FROM unified_deal_base udm
        LEFT JOIN customer_first_purchases fp
        ON udm.customer_id = fp.customer_id
        AND udm.asin = fp.asin
        AND udm.brand_code = fp.brand_code
        AND udm.company_code = fp.company_code
        AND udm.gl_product_group = fp.gl_product_group
);


---------------------------------------- ASIN LEVEL ----------------------------------------

DROP TABLE IF EXISTS deal_asins;
CREATE TEMP TABLE deal_asins AS (
    SELECT DISTINCT
        asin,
        event_name,
        event_year
    FROM unified_daily_metrics 
    WHERE period_type = 'DEAL'
);

/*************************
Deal Period Metrics
*************************/
DROP TABLE IF EXISTS deal_metrics;
CREATE TEMP TABLE deal_metrics AS (
    SELECT 
        udm.asin,
        udm.item_name,
        udm.gl_product_group,
        udm.gl_product_group_name,
        udm.brand_code,
        udm.brand_name,
        udm.vendor_code,
        udm.company_code,
        udm.company_name,
        udm.event_name,
        udm.event_year,
        udm.event_month,
        MIN(udm.promo_start_date) as promo_start_date,
        MAX(udm.promo_end_date) as promo_end_date,
        MAX(udm.event_duration_days) as event_duration_days,
        
        -- Deal period metrics
        SUM(CASE WHEN udm.period_type = 'DEAL' 
            THEN udm.shipped_units END)/MAX(udm.event_duration_days) as daily_deal_shipped_units,
        SUM(CASE WHEN udm.period_type = 'DEAL' 
            THEN udm.revenue_share_amt END)/MAX(udm.event_duration_days) as daily_deal_ops,
        
        -- Total customers
        COUNT(DISTINCT CASE 
            WHEN udm.period_type = 'DEAL' 
            THEN udm.customer_id 
        END)/MAX(udm.event_duration_days) as daily_deal_customers,
        
        -- New customers (using pre-calculated first purchase flags)
        SUM(CASE 
            WHEN udm.period_type = 'DEAL' AND udm.is_first_asin_purchase = 1
            THEN 1 ELSE 0 
        END)/MAX(udm.event_duration_days) as daily_deal_new_customers,
        
        -- SNS subscribers
        AVG(CASE WHEN udm.period_type = 'DEAL' 
            THEN udm.daily_sns_subscribers END) as daily_deal_sns_subscribers
            
    FROM unified_daily_metrics udm
    GROUP BY 
        udm.asin,
        udm.item_name,
        udm.gl_product_group,
        udm.gl_product_group_name,
        udm.brand_code,
        udm.brand_name,
        udm.vendor_code,
        udm.company_code,
        udm.company_name,
        udm.event_name,
        udm.event_year,
        udm.event_month
);

/*************************
Pre-Deal Period Metrics
*************************/
DROP TABLE IF EXISTS pre_deal_metrics;
CREATE TEMP TABLE pre_deal_metrics AS (
    SELECT 
        udm.asin,
        udm.event_name,
        udm.event_year,
        
        SUM(CASE WHEN udm.period_type = 'PRE_DEAL' 
            THEN shipped_units END)/29 as daily_pre_deal_shipped_units,
        SUM(CASE WHEN udm.period_type = 'PRE_DEAL' 
            THEN revenue_share_amt END)/29.0 as daily_pre_deal_ops,
        
        COUNT(DISTINCT CASE 
            WHEN udm.period_type = 'PRE_DEAL' 
            THEN udm.customer_id 
        END)/29 as daily_pre_deal_customers,
        
        SUM(CASE 
            WHEN udm.period_type = 'PRE_DEAL' AND udm.is_first_asin_purchase = 1
            THEN 1 ELSE 0 
        END)/29 as daily_pre_deal_new_customers,
        
        AVG(CASE WHEN udm.period_type = 'PRE_DEAL' 
            THEN daily_sns_subscribers END) as daily_pre_deal_sns_subscribers
            
    FROM unified_daily_metrics udm
    WHERE udm.period_type = 'PRE_DEAL'
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
        (d.daily_deal_sns_subscribers - p.daily_pre_deal_sns_subscribers) as delta_daily_sns_subscribers  -- Fixed this line
    
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
        cast(event_year as varchar) as event_year,
        cast(event_month as varchar) as event_month,
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
        
        -- Current Year Delta Metrics
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
        ly_daily_pre_deal_sns_subscribers,

        -- Last Year Delta Metrics
        (ly_daily_deal_shipped_units - ly_daily_pre_deal_shipped_units) as ly_delta_daily_shipped_units,
        (ly_daily_deal_ops - ly_daily_pre_deal_ops) as ly_delta_daily_ops,
        (ly_daily_deal_customers - ly_daily_pre_deal_customers) as ly_delta_daily_customers,
        (ly_daily_deal_new_customers - ly_daily_pre_deal_new_customers) as ly_delta_daily_new_customers,
        (ly_daily_deal_sns_subscribers - ly_daily_pre_deal_sns_subscribers) as ly_delta_daily_sns_subscribers,

        -- Year-over-Year Delta Comparisons
        (delta_daily_shipped_units - (ly_daily_deal_shipped_units - ly_daily_pre_deal_shipped_units)) as yoy_delta_daily_shipped_units,
        (delta_daily_ops - (ly_daily_deal_ops - ly_daily_pre_deal_ops)) as yoy_delta_daily_ops,
        (delta_daily_customers - (ly_daily_deal_customers - ly_daily_pre_deal_customers)) as yoy_delta_daily_customers,
        (delta_daily_new_customers - (ly_daily_deal_new_customers - ly_daily_pre_deal_new_customers)) as yoy_delta_daily_new_customers,
        (delta_daily_sns_subscribers - (ly_daily_deal_sns_subscribers - ly_daily_pre_deal_sns_subscribers)) as yoy_delta_daily_sns_subscribers

    FROM final_asin_metrics
    
    ORDER BY 
        event_year DESC,
        event_name,
        daily_deal_ops DESC
);


---------------------------------------- BRAND LEVEL --------------------------------------- 

DROP TABLE IF EXISTS deal_brands;
CREATE TEMP TABLE deal_brands AS (
    SELECT DISTINCT
        brand_code,
        event_name,
        event_year
    FROM unified_daily_metrics 
    WHERE period_type = 'DEAL'
);


/*************************
Brand Level Metrics
*************************/
DROP TABLE IF EXISTS deal_metrics_brand;
CREATE TEMP TABLE deal_metrics_brand AS (
    SELECT 
        udm.brand_code,
        udm.brand_name,
        udm.vendor_code,
        udm.company_code,
        udm.company_name,
        udm.gl_product_group,
        udm.gl_product_group_name,
        udm.event_name,
        udm.event_year,
        udm.event_month,
        MIN(udm.promo_start_date) as promo_start_date,
        MAX(udm.promo_end_date) as promo_end_date,
        MAX(udm.promo_end_date) - MIN(udm.promo_start_date) + 1 as event_duration_days,
        
        -- Deal period metrics
        SUM(CASE WHEN udm.period_type = 'DEAL' 
            THEN udm.shipped_units END)/(MAX(udm.promo_end_date) - MIN(udm.promo_start_date) + 1) as daily_deal_shipped_units,
        SUM(CASE WHEN udm.period_type = 'DEAL' 
            THEN udm.revenue_share_amt END)/(MAX(udm.promo_end_date) - MIN(udm.promo_start_date) + 1) as daily_deal_ops,
        
        -- Total customers
        COUNT(DISTINCT CASE 
            WHEN udm.period_type = 'DEAL' 
            THEN udm.customer_id 
        END)/(MAX(udm.promo_end_date) - MIN(udm.promo_start_date) + 1) as daily_deal_customers,
        
        -- New customers
        COUNT(DISTINCT CASE 
            WHEN udm.period_type = 'DEAL' AND udm.is_first_brand_purchase = 1
            THEN udm.customer_id 
        END)/(MAX(udm.promo_end_date) - MIN(udm.promo_start_date) + 1) as daily_deal_new_customers,

        -- SNS subscribers
        AVG(CASE WHEN udm.period_type = 'DEAL' 
            THEN udm.daily_sns_subscribers END) as daily_deal_sns_subscribers
            
    FROM unified_daily_metrics udm
    
    WHERE udm.brand_code IS NOT NULL
    GROUP BY 
        udm.brand_code,
        udm.brand_name,
        udm.vendor_code,
        udm.company_code,
        udm.company_name,
        udm.gl_product_group,
        udm.gl_product_group_name,
        udm.event_name,
        udm.event_year,
        udm.event_month
);


-- Pre-deal metrics for Brand level
DROP TABLE IF EXISTS pre_deal_metrics_brand;
CREATE TEMP TABLE pre_deal_metrics_brand AS (
    SELECT 
        udm.brand_code,
        udm.event_name,
        udm.event_year,
        
        SUM(CASE WHEN udm.period_type = 'PRE_DEAL' 
            THEN shipped_units END)/29 as daily_pre_deal_shipped_units,
        SUM(CASE WHEN udm.period_type = 'PRE_DEAL' 
            THEN revenue_share_amt END)/29.0 as daily_pre_deal_ops,
        
        COUNT(DISTINCT CASE 
            WHEN udm.period_type = 'PRE_DEAL' 
            THEN udm.customer_id 
        END)/29 as daily_pre_deal_customers,
        
        COUNT(DISTINCT CASE 
            WHEN udm.period_type = 'PRE_DEAL' AND udm.is_first_brand_purchase = 1
            THEN udm.customer_id 
        END)/29 as daily_pre_deal_new_customers,

        
        AVG(CASE WHEN udm.period_type = 'PRE_DEAL' 
            THEN daily_sns_subscribers END) as daily_pre_deal_sns_subscribers
            
    FROM unified_daily_metrics udm

    WHERE udm.period_type = 'PRE_DEAL'
    GROUP BY 1,2,3
);



-- Combine and calculate deltas for brand
DROP TABLE IF EXISTS deal_growth_brand;
CREATE TEMP TABLE deal_growth_brand AS (
    SELECT 
        d.*,
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
        
        -- Current Year Delta Metrics
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
        COALESCE(ly_daily_pre_deal_sns_subscribers, 0) as ly_daily_pre_deal_sns_subscribers,

        -- Last Year Delta Metrics
        COALESCE(ly_daily_deal_shipped_units, 0) - COALESCE(ly_daily_pre_deal_shipped_units, 0) as ly_delta_daily_shipped_units,
        COALESCE(ly_daily_deal_ops, 0) - COALESCE(ly_daily_pre_deal_ops, 0) as ly_delta_daily_ops,
        COALESCE(ly_daily_deal_customers, 0) - COALESCE(ly_daily_pre_deal_customers, 0) as ly_delta_daily_customers,
        COALESCE(ly_daily_deal_new_customers, 0) - COALESCE(ly_daily_pre_deal_new_customers, 0) as ly_delta_daily_new_customers,
        COALESCE(ly_daily_deal_sns_subscribers, 0) - COALESCE(ly_daily_pre_deal_sns_subscribers, 0) as ly_delta_daily_sns_subscribers,

        -- Year-over-Year Delta Comparisons
        COALESCE(delta_daily_shipped_units, 0) - (COALESCE(ly_daily_deal_shipped_units, 0) - COALESCE(ly_daily_pre_deal_shipped_units, 0)) as yoy_delta_daily_shipped_units,
        COALESCE(delta_daily_ops, 0) - (COALESCE(ly_daily_deal_ops, 0) - COALESCE(ly_daily_pre_deal_ops, 0)) as yoy_delta_daily_ops,
        COALESCE(delta_daily_customers, 0) - (COALESCE(ly_daily_deal_customers, 0) - COALESCE(ly_daily_pre_deal_customers, 0)) as yoy_delta_daily_customers,
        COALESCE(delta_daily_new_customers, 0) - (COALESCE(ly_daily_deal_new_customers, 0) - COALESCE(ly_daily_pre_deal_new_customers, 0)) as yoy_delta_daily_new_customers,
        COALESCE(delta_daily_sns_subscribers, 0) - (COALESCE(ly_daily_deal_sns_subscribers, 0) - COALESCE(ly_daily_pre_deal_sns_subscribers, 0)) as yoy_delta_daily_sns_subscribers

    FROM final_brand_metrics
    ORDER BY 
        event_year DESC,
        event_name,
        daily_deal_ops DESC
);


---------------------------------------- COMPANY-GL LEVEL ----------------------------------------

DROP TABLE IF EXISTS deal_companies;
CREATE TEMP TABLE deal_companies AS (
    SELECT DISTINCT
        company_code,
        gl_product_group,
        event_name,
        event_year
    FROM unified_daily_metrics 
    WHERE period_type = 'DEAL'
);



DROP TABLE IF EXISTS deal_metrics_company;
CREATE TEMP TABLE deal_metrics_company AS (
    SELECT 
        udm.company_code,
        udm.company_name,
        udm.gl_product_group,
        udm.gl_product_group_name,
        udm.event_name,
        udm.event_year,
        udm.event_month,
        MIN(udm.promo_start_date) as promo_start_date,
        MAX(udm.promo_end_date) as promo_end_date,
        MAX(udm.promo_end_date) - MIN(udm.promo_start_date) + 1 as event_duration_days,
        
        -- Deal period metrics
        SUM(CASE WHEN udm.period_type = 'DEAL' 
            THEN udm.shipped_units END)/(MAX(udm.promo_end_date) - MIN(udm.promo_start_date) + 1) as daily_deal_shipped_units,
        SUM(CASE WHEN udm.period_type = 'DEAL' 
            THEN udm.revenue_share_amt END)/(MAX(udm.promo_end_date) - MIN(udm.promo_start_date) + 1) as daily_deal_ops,
        
        -- Total customers
        COUNT(DISTINCT CASE 
            WHEN udm.period_type = 'DEAL' 
            THEN udm.customer_id 
        END)/(MAX(udm.promo_end_date) - MIN(udm.promo_start_date) + 1) as daily_deal_customers,
        
        -- New customers

        COUNT(DISTINCT CASE 
            WHEN udm.period_type = 'DEAL' AND udm.is_first_company_purchase = 1
            THEN udm.customer_id 
        END)/(MAX(udm.promo_end_date) - MIN(udm.promo_start_date) + 1) as daily_deal_new_customers,
        -- SNS subscribers
        AVG(CASE WHEN udm.period_type = 'DEAL' 
            THEN udm.daily_sns_subscribers END) as daily_deal_sns_subscribers
            
    FROM unified_daily_metrics udm


    WHERE udm.company_code IS NOT NULL
    GROUP BY 
        udm.company_code,
        udm.company_name,
        udm.gl_product_group,
        udm.gl_product_group_name,
        udm.event_name,
        udm.event_year,
        udm.event_month
);


-- Pre-deal metrics for Company-GL level
DROP TABLE IF EXISTS pre_deal_metrics_company;
CREATE TEMP TABLE pre_deal_metrics_company AS (
    SELECT 
        udm.company_code,
        udm.gl_product_group,
        udm.event_name,
        udm.event_year,
        
        SUM(CASE WHEN udm.period_type = 'PRE_DEAL' 
            THEN shipped_units END)/29 as daily_pre_deal_shipped_units,
        SUM(CASE WHEN udm.period_type = 'PRE_DEAL' 
            THEN revenue_share_amt END)/29.0 as daily_pre_deal_ops,
        
        COUNT(DISTINCT CASE 
            WHEN udm.period_type = 'PRE_DEAL' 
            THEN udm.customer_id 
        END)/29 as daily_pre_deal_customers,
        
        COUNT(DISTINCT CASE 
            WHEN udm.period_type = 'PRE_DEAL' AND udm.is_first_company_purchase = 1
            THEN udm.customer_id 
        END)/29 as daily_pre_deal_new_customers,   

        AVG(CASE WHEN udm.period_type = 'PRE_DEAL' 
            THEN daily_sns_subscribers END) as daily_pre_deal_sns_subscribers
            
    FROM unified_daily_metrics udm
    
    WHERE udm.period_type = 'PRE_DEAL'
    GROUP BY 1,2,3,4
);


-- Combine and calculate deltas for company-GL
DROP TABLE IF EXISTS deal_growth_company;
CREATE TEMP TABLE deal_growth_company AS (
    SELECT 
        d.*,
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
        
        -- Current Year Delta Metrics
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
        COALESCE(ly_daily_pre_deal_sns_subscribers, 0) as ly_daily_pre_deal_sns_subscribers,

        -- Last Year Delta Metrics
        COALESCE(ly_daily_deal_shipped_units, 0) - COALESCE(ly_daily_pre_deal_shipped_units, 0) as ly_delta_daily_shipped_units,
        COALESCE(ly_daily_deal_ops, 0) - COALESCE(ly_daily_pre_deal_ops, 0) as ly_delta_daily_ops,
        COALESCE(ly_daily_deal_customers, 0) - COALESCE(ly_daily_pre_deal_customers, 0) as ly_delta_daily_customers,
        COALESCE(ly_daily_deal_new_customers, 0) - COALESCE(ly_daily_pre_deal_new_customers, 0) as ly_delta_daily_new_customers,
        COALESCE(ly_daily_deal_sns_subscribers, 0) - COALESCE(ly_daily_pre_deal_sns_subscribers, 0) as ly_delta_daily_sns_subscribers,

        -- Year-over-Year Delta Comparisons
        COALESCE(delta_daily_shipped_units, 0) - (COALESCE(ly_daily_deal_shipped_units, 0) - COALESCE(ly_daily_pre_deal_shipped_units, 0)) as yoy_delta_daily_shipped_units,
        COALESCE(delta_daily_ops, 0) - (COALESCE(ly_daily_deal_ops, 0) - COALESCE(ly_daily_pre_deal_ops, 0)) as yoy_delta_daily_ops,
        COALESCE(delta_daily_customers, 0) - (COALESCE(ly_daily_deal_customers, 0) - COALESCE(ly_daily_pre_deal_customers, 0)) as yoy_delta_daily_customers,
        COALESCE(delta_daily_new_customers, 0) - (COALESCE(ly_daily_deal_new_customers, 0) - COALESCE(ly_daily_pre_deal_new_customers, 0)) as yoy_delta_daily_new_customers,
        COALESCE(delta_daily_sns_subscribers, 0) - (COALESCE(ly_daily_deal_sns_subscribers, 0) - COALESCE(ly_daily_pre_deal_sns_subscribers, 0)) as yoy_delta_daily_sns_subscribers

    FROM final_company_metrics
    ORDER BY 
        event_year DESC,
        event_name,
        daily_deal_ops DESC
);


-------------------------------- GL LEVELS ----------------------------------------

DROP TABLE IF EXISTS deal_gls;
CREATE TEMP TABLE deal_gls AS (
    SELECT DISTINCT
        gl_product_group,
        event_name,
        event_year
    FROM unified_daily_metrics 
    WHERE period_type = 'DEAL'
);


/*************************
GL Level Metrics
*************************/
DROP TABLE IF EXISTS deal_metrics_gl;
CREATE TEMP TABLE deal_metrics_gl AS (
    SELECT 
        udm.gl_product_group,
        udm.gl_product_group_name,
        udm.event_name,
        udm.event_year,
        udm.event_month,
        MIN(udm.promo_start_date) as promo_start_date,
        MAX(udm.promo_end_date) as promo_end_date,
        MAX(udm.promo_end_date) - MIN(udm.promo_start_date) + 1 as event_duration_days,
        
        -- Deal period metrics
        SUM(CASE WHEN udm.period_type = 'DEAL' 
            THEN udm.shipped_units END)/(MAX(udm.promo_end_date) - MIN(udm.promo_start_date) + 1) as daily_deal_shipped_units,
        SUM(CASE WHEN udm.period_type = 'DEAL' 
            THEN udm.revenue_share_amt END)/(MAX(udm.promo_end_date) - MIN(udm.promo_start_date) + 1) as daily_deal_ops,
        
        -- Total customers
        COUNT(DISTINCT CASE 
            WHEN udm.period_type = 'DEAL' 
            THEN udm.customer_id 
        END)/(MAX(udm.promo_end_date) - MIN(udm.promo_start_date) + 1) as daily_deal_customers,
        
        -- New customers
        COUNT(DISTINCT CASE 
            WHEN udm.period_type = 'DEAL' AND udm.is_first_gl_purchase = 1
            THEN udm.customer_id 
        END)/(MAX(udm.promo_end_date) - MIN(udm.promo_start_date) + 1) as daily_deal_new_customers,

        -- SNS subscribers
        AVG(CASE WHEN udm.period_type = 'DEAL' 
            THEN udm.daily_sns_subscribers END) as daily_deal_sns_subscribers
            
    FROM unified_daily_metrics udm


    WHERE udm.gl_product_group IS NOT NULL
    GROUP BY 
        udm.gl_product_group,
        udm.gl_product_group_name,
        udm.event_name,
        udm.event_year,
        udm.event_month
);



DROP TABLE IF EXISTS pre_deal_metrics_gl;
CREATE TEMP TABLE pre_deal_metrics_gl AS (
    SELECT 
        udm.gl_product_group,
        udm.event_name,
        udm.event_year,
        
        SUM(CASE WHEN udm.period_type = 'PRE_DEAL' 
            THEN shipped_units END)/29 as daily_pre_deal_shipped_units,
        SUM(CASE WHEN udm.period_type = 'PRE_DEAL' 
            THEN revenue_share_amt END)/29.0 as daily_pre_deal_ops,
        
        COUNT(DISTINCT CASE 
            WHEN udm.period_type = 'PRE_DEAL' 
            THEN udm.customer_id 
        END)/29 as daily_pre_deal_customers,

        COUNT(DISTINCT CASE 
            WHEN udm.period_type = 'PRE_DEAL' AND udm.is_first_gl_purchase = 1
            THEN udm.customer_id 
        END)/29 as daily_pre_deal_new_customers,

        AVG(CASE WHEN udm.period_type = 'PRE_DEAL' 
            THEN daily_sns_subscribers END) as daily_pre_deal_sns_subscribers
            
    FROM unified_daily_metrics udm


    WHERE udm.period_type = 'PRE_DEAL'
    GROUP BY 1,2,3
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
        -- promo_start_date,
        -- promo_end_date,
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
        
        -- Current Year Delta Metrics
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
        COALESCE(ly_daily_pre_deal_sns_subscribers, 0) as ly_daily_pre_deal_sns_subscribers,

        -- Last Year Delta Metrics
        COALESCE(ly_daily_deal_shipped_units, 0) - COALESCE(ly_daily_pre_deal_shipped_units, 0) as ly_delta_daily_shipped_units,
        COALESCE(ly_daily_deal_ops, 0) - COALESCE(ly_daily_pre_deal_ops, 0) as ly_delta_daily_ops,
        COALESCE(ly_daily_deal_customers, 0) - COALESCE(ly_daily_pre_deal_customers, 0) as ly_delta_daily_customers,
        COALESCE(ly_daily_deal_new_customers, 0) - COALESCE(ly_daily_pre_deal_new_customers, 0) as ly_delta_daily_new_customers,
        COALESCE(ly_daily_deal_sns_subscribers, 0) - COALESCE(ly_daily_pre_deal_sns_subscribers, 0) as ly_delta_daily_sns_subscribers,

        -- Year-over-Year Delta Comparisons
        COALESCE(delta_daily_shipped_units, 0) - (COALESCE(ly_daily_deal_shipped_units, 0) - COALESCE(ly_daily_pre_deal_shipped_units, 0)) as yoy_delta_daily_shipped_units,
        COALESCE(delta_daily_ops, 0) - (COALESCE(ly_daily_deal_ops, 0) - COALESCE(ly_daily_pre_deal_ops, 0)) as yoy_delta_daily_ops,
        COALESCE(delta_daily_customers, 0) - (COALESCE(ly_daily_deal_customers, 0) - COALESCE(ly_daily_pre_deal_customers, 0)) as yoy_delta_daily_customers,
        COALESCE(delta_daily_new_customers, 0) - (COALESCE(ly_daily_deal_new_customers, 0) - COALESCE(ly_daily_pre_deal_new_customers, 0)) as yoy_delta_daily_new_customers,
        COALESCE(delta_daily_sns_subscribers, 0) - (COALESCE(ly_daily_deal_sns_subscribers, 0) - COALESCE(ly_daily_pre_deal_sns_subscribers, 0)) as yoy_delta_daily_sns_subscribers

    FROM final_gl_metrics
    ORDER BY 
        event_year DESC,
        event_name,
        daily_deal_ops DESC
);

-- Grant permissions for all tables
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_asin_level TO PUBLIC;
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_brand_level TO PUBLIC;
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_company_level TO PUBLIC;
GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_gl_level TO PUBLIC;
-- GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_event_level TO PUBLIC;
