/*************************
Base Orders Query
- Gets order data for consumables categories
- Includes only retail merchant orders with shipped units > 0 
*************************/
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
        AND o.asin in (
    'B000255QWA','B0002563QI','B0002563RW','B00025640S','B0002565NY','B0002565PW','B0002565Q6','B0002565RA','B0002565SY','B0002565TI',
    'B0002566WE','B0002566WO','B0002566YM','B00025K10C','B00025Z6SO','B0002APMEC','B0002DHY6S','B0002DHZIU','B0002DHZKS','B0002DHZSU',
    'B0002DIFIE','B0002DJOV6','B0006JM09E','B0006JM09O','B0009YD6XA','B0009YHSAC','B0009YHSE8','B000HHLHDK','B000HHLKRI','B000ICNM96',
    'B000IWXHMI','B000J3I0Q4','B000N31TYU','B000NIG7R4','B000NRS31S','B000NRVLIU','B000NRXB5G','B000OQM7J2','B000OQO69Q','B000OQRLHK',
    'B000OQRLL6','B000V7KM32','B001CHSJZS','B001CHXJSK','B001CJC8X0','B001D73ZWY','B001DCZUO0','B001KN959I','B001LISOLM','B001LUHBDC',
    'B001LUOBNU','B001MSS358','B001RE9ZTK','B001XRSR1S','B001XRXVPU','B0024EFYU6','B0024EFZAA','B002DVTDRU','B002DVVICI','B002MW6CXS',
    'B002MW9YU6','B002RB752G','B002RBAT24','B002RBCT0O','B002RBEGC8','B002TQOFQS','B003907Q96','B003AYX474','B003JVR1J0','B003JVYY7M',
    'B003SNCEZU','B003WRHRQS','B00474FI84','B00474FPZU','B00474FQJ0','B004N1NAMW','B004PB8SMM','B004PB8UF2','B004PB93AI','B004PBCS82',
    'B004QDA8HW','B004QDBAUQ','B004YK5HAK','B004ZR9TDI','B0053PR7P8','B005FTW6Z8','B0062Z0S3G','B0062Z0UOI','B0062Z0UVG','B006L49H30',
    'B006WC01O0','B006WC1F0E','B007HVNKEI','B007R5KN80','B007TGMIG2','B007TGMJXE','B007TGMLU0','B007TGMLVO','B007TGMLW8','B007TGMLXM',
    'B007TU2Y0I','B009II9YSO','B00A0MSPLY','B00BQ3GXY2','B00BS95YLM','B00BUFSKYW','B00CQ7JRAY','B00DCVPKWM','B00DCVPL4E','B00DCVPLFS',
    'B00DS3603C','B00FJ04AZW','B00FJ04B0Q','B00FJ04B10','B00FJ04BO2','B00FJ04BOW','B00FJ04CH8','B00FJ04CHS','B00FJ04CIM','B00FJ04CJQ',
    'B00GOFSB6U','B00GUG1AKW','B00IK5S0RC','B00J4BMCOS','B00JGSM0VE','B00JIAL34A','B00JN9I1KA','B00KKKJ7EA','B00KVL58KA','B00KVL5DU0',
    'B00M4Q2F92','B00M4Q69J4','B00NETRY1W','B00NETRZIO','B00OYHF7RQ','B00TF1EF3I','B00ZWHBA4Q','B00ZWHBMVM','B00ZWHBMYY','B00ZWHCQH6',
    'B00ZWHCQK8','B00ZWHE8EK','B00ZWHEDA4','B010PLGHGE','B010PLGI5O','B01C8PH0OI','B01C8PH3JU','B01KBV4TL8','B01N0AFPGY','B06VXQQKRG',
    'B06W2PNFCM','B071GQVTSZ','B0721Q1NSK','B072KKF846','B072KKF8S5','B072N56ZYX','B072N571KJ','B072R4T783','B0744RP9CY','B0749KHF58',
    'B0749LRR8P','B075MKTGJM','B075YDYCBN','B0793PT78B','B07BJ93VT7','B07BJ977FH','B07BJ9GTLQ','B07BJ9GX4J','B07BJ9TPS8','B07DF32PG6',
    'B07DG7RZVC','B07DPX6FB9','B07DQ4WWKB','B07DQ5472S','B07FTPRZZL','B07FTQSK84','B07FTZGJ5P','B07G4KCPVM','B07G5FZ7R1','B07G5FZ7R6',
    'B07G5GJHMS','B07G5S5MLK','B07G5S74R2','B07G5SBQZQ','B07G5SBQZT','B07G5WVF2Z','B07G5WZRM9','B07G5X4FQ6','B07G5Y75VG','B07G5Y9DHC',
    'B07G7GRM2M','B07GLJWHRV','B07JC6LST2','B07JL29W5W','B07JM9JTMR','B07JMGK48T','B07JXJKHHF','B07JXJQXPT','B07JXJSGTT','B07JXJXS5D',
    'B07L24R5K4','B07L25PZK8','B07L25S7GJ','B07MNGB19P','B07MNGL2X2','B07MZBX5ZW','B07MZBZT5D','B07MZCY821','B07MZD13SR','B07MZD3ZVF',
    'B07MZD93QQ','B07MZDGF5M','B07MZDTG76','B07MZDTHPV','B07NGM3QRN','B07NV5C3PF','B07P29YJ23','B07P3FPFNL','B07P3FQ1W5','B07PK5ZCZ6',
    'B07RGSWB4C','B07RR1GGD1','B07RWDR955','B07RWVM64F','B07WXQKF1X','B07YXHVWRZ','B081W2M14X','B086G787XZ','B086G8632M','B086GBV9HY',
    'B086GBV9J4','B086GQB4W3','B086QGMXWT','B087DP8TGR','B087DQ5WWR','B087DQ65ZY','B08BJ9FQY7','B08BJB7FX4','B08BJBYB4S','B08BMG5Y7Z',
    'B08BMHBNQB','B08BMK1P73','B08BMK42HJ','B08BMM6R1Z','B08BMP99SC','B08BMQZNYN','B08BMRRSRW','B08BMV48SF','B08BMW1RC1','B08LF1128L',
    'B08R6L3PRF','B08W9Q28V2','B091D34GW1','B091D3DTWS','B091D45L3R','B091D46799','B091D4WZW5','B091D5DZVB','B091D6SN4S','B09B49SKQX',
    'B09B4BBLVJ','B09B4BP37B','B09B4C3LBZ','B09B4C6PQW','B09B4C9XG3','B09B4CQBR8','B09B4CTTCV','B09B4CZVFK','B09B4D3PC1','B09B4DW3VY',
    'B09B4FHWXW','B09B4QHGD9','B09CG1QQK5','B09FMZWMYB','B09KQVCC67','B09RGRXR7T','B09Y2HNMMF','B09Y2JMSRH','B09Y7M25BT','B09Z78XZZL',
    'B09Z7B3TQ2','B0B29D333C','B0B29G9DLG','B0B2Q48WC5','B0B2Q58Q7S','B0B2S9LPHL','B0B2SB6XRQ','B0B2SBKNJT','B0B2SCCM4L','B0B2SCXTLM',
    'B0B2SD1HJB','B0B2Y7BKWJ','B0B33B944N','B0B33HG7F7','B0B33JRR3N','B0B33PKP3B','B0B33YXBWH','B0B6CQLD4G','B0B6CV95NF','B0BBSHCF7G',
    'B0BFXGDDHY','B0BFXJ16SC','B0BFXJHMNG','B0BFXJQD24','B0BG36PLBL','B0BG379894','B0BG37CTVY','B0BG39TL34','B0BLHGQ3CC','B0BLHGVDNC',
    'B0BSXV1KP4','B0BT26D22Z','B0BT26S15N','B0BTTGP6JF','B0BVVL6R1C','B0C3DH37FB','B0C3DHM86P','B0CCB4XW1M','B0CCB6GSGX','B0CDM5X8SG',
    'B0CG2D8TXS','B0CNWGX93J','B0CNWHTWXX','B0CNWJJFRD','B0CSG5S2SS','B0CSS9QXP8','B0CVLK2KSP','B0CVLWWB7P','B0CVQSHR6Q','B0CYT8B59L',
    'B0D1MSN4QJ','B0D1MSQQSZ','B0D1MSVLH9','B0D1MSW3G2','B0D1MVGQ1R','B0D1MWYJNM','B0D1MX4GRR','B0D1MYMJCX','B0D1N6HBWB','B0DVPSKMJ9',
    'B0DVPWMHC5'
    )
        AND o.marketplace_id = 7
        AND o.shipped_units > 0
        AND o.is_retail_merchant = 'Y'
        AND o.order_datetime BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '386 days'
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
*************************/
DROP TABLE IF EXISTS pre_deal_date_ranges;
CREATE TEMP TABLE pre_deal_date_ranges AS (
    SELECT 
        asin,
        promo_start_date,
        promo_end_date,
        event_name,
        event_year,
        event_month,
        promo_start_date - interval '27 days' AS pre_deal_start_date,
        promo_start_date - interval '14 days' AS pre_deal_end_date
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
    WHERE b.order_date BETWEEN pdr.pre_deal_start_date 
        AND pdr.pre_deal_end_date
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
Per customer per day summary - Split by period type but using same first_purchases
*************************/
DROP TABLE IF EXISTS deal_daily_summary;
CREATE TEMP TABLE deal_daily_summary AS (
    SELECT 
        o.asin,
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
        CASE 
            WHEN o.order_date = fp.first_purchase_date THEN 1 
            ELSE 0 
        END AS is_first_brand_purchase,
        o.shipped_units,
        o.revenue_share_amt
    FROM deal_orders o
        LEFT JOIN first_purchases fp
        ON o.customer_id = fp.customer_id
        AND o.brand_code = fp.brand_code
);


-- pre deal is nto joined right
DROP TABLE IF EXISTS pre_deal_daily_summary;
CREATE TEMP TABLE pre_deal_daily_summary AS (
    SELECT 
        o.asin,
        o.gl_product_group,
        o.brand_code,
        o.brand_name,
        o.event_name,
        o.promo_start_date,
        o.promo_end_date,
        o.period_type,
        o.order_date,
        o.customer_id,
        CASE 
            WHEN o.order_date = fp.first_purchase_date THEN 1 
            ELSE 0 
        END AS is_first_brand_purchase,
        o.shipped_units,
        o.revenue_share_amt
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
            gl_product_group,
            brand_code,
            brand_name,
            event_name,
            promo_start_date,
            promo_end_date,
            DATE_PART('month', promo_start_date) as event_month,
            DATE_PART('year', promo_start_date) as event_year,
            CASE 
                WHEN promo_end_date >= TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') 
                    THEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - promo_start_date + 1
                ELSE promo_end_date - promo_start_date + 1
            END as event_duration_days,
            -- total
            SUM(shipped_units) as shipped_units,
            SUM(revenue_share_amt) as revenue,
            COUNT(DISTINCT customer_id) as total_customers,
            COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END) as new_customers,
            COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END) as return_customers
        FROM deal_daily_summary
        -- Add grouping by event context
        GROUP BY 1,2,3,4,5,6,7
    )
    SELECT 
        bm.*,
        mam.dama_mfg_vendor_code as vendor_code,
        -- daily
        shipped_units/event_duration_days as daily_deal_shipped_units,
        revenue/event_duration_days as daily_deal_ops,
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
Pre-deal metrics calculation - Include event context
*************************/
DROP TABLE IF EXISTS pre_deal_metrics;
CREATE TEMP TABLE pre_deal_metrics AS (
    SELECT 
        asin,
        event_name,
        promo_start_date,
        -- order_date,
        SUM(shipped_units)/14 as daily_pre_deal_shipped_units,  
        SUM(revenue_share_amt)/14 as daily_pre_deal_revenue,    
        COUNT(DISTINCT customer_id)/14 as daily_pre_deal_total_customers,  
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 1 THEN customer_id END)/14 as daily_pre_deal_new_customers, 
        COUNT(DISTINCT CASE WHEN is_first_brand_purchase = 0 THEN customer_id END)/14 as daily_pre_deal_return_customers
    FROM pre_deal_daily_summary
    GROUP BY 1,2,3
);


/*************************
Final table creation
*************************/
DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_asin_may29;
CREATE TABLE pm_sandbox_aqxiao.ntb_asin_may29 AS (
    SELECT 
        d.asin,
        CASE 
            WHEN d.gl_product_group = 510 THEN 'Lux Beauty'
            WHEN d.gl_product_group = 364 THEN 'Personal Care Appliances'    
            WHEN d.gl_product_group = 325 THEN 'Grocery'
            WHEN d.gl_product_group = 199 THEN 'Pet'
            WHEN d.gl_product_group = 194 THEN 'Beauty'
            WHEN d.gl_product_group = 121 THEN 'HPC'
            WHEN d.gl_product_group = 75 THEN 'Baby'    
        END as gl_product_group_name,
        d.gl_product_group,
        d.vendor_code,
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
        d.daily_deal_total_customers as daily_deal_total_customers,
        d.daily_deal_new_customers as daily_deal_new_customers,
        d.daily_deal_return_customers as daily_deal_return_customers,
        
        p.daily_pre_deal_shipped_units as daily_pre_deal_shipped_units,
        p.daily_pre_deal_revenue as daily_pre_deal_revenue,
        p.daily_pre_deal_total_customers as daily_pre_deal_total_customers,
        p.daily_pre_deal_new_customers as daily_pre_deal_new_customers,
        p.daily_pre_deal_return_customers as daily_pre_deal_return_customers,

        -- Growth calculations (comparing daily averages)
        ROUND(
            CASE 
                WHEN COALESCE(p.daily_pre_deal_new_customers, 0) = 0 
                THEN  ((d.daily_deal_new_customers::FLOAT / 0.000000001) - 1) * 100
                ELSE ((d.daily_deal_new_customers::FLOAT / p.daily_pre_deal_new_customers) - 1) * 100
            END, 
            2
        ) as daily_new_customer_growth_pct,
        
        ROUND(
            CASE 
                WHEN COALESCE(p.daily_pre_deal_return_customers, 0) = 0 
                THEN ((d.daily_deal_return_customers::FLOAT / 0.000000001) - 1) * 100
                ELSE ((d.daily_deal_return_customers::FLOAT / p.daily_pre_deal_return_customers) - 1) * 100
            END,
            2
        ) as daily_return_customer_growth_pct
    FROM deal_metrics d
        LEFT JOIN pre_deal_metrics p
        ON d.asin = p.asin
        AND d.event_name = p.event_name
        AND d.promo_start_date = p.promo_start_date
    WHERE d.promo_start_date IS NOT NULL
    ORDER BY 
        d.promo_start_date DESC,
        d.daily_deal_ops * d.event_duration_days DESC
);


-- to be audited


/*************************
EVENT ANALYSIS (BRAND LEVEL)
Calculates brand level separately for customer counts to avoid duplicates
*************************/
-- DROP TABLE IF EXISTS event_analysis_brand;
-- CREATE TEMP TABLE event_analysis_brand AS (
--     WITH brand_level_customers AS (
--         SELECT DISTINCT
--             o.gl_product_group,
--             o.brand_code,
--             o.brand_name,
--             o.event_name,
--             o.promo_start_date,
--             o.promo_end_date,
--             o.event_month,
--             o.event_year,
--             o.period_type,
--             o.customer_id,
--             o.order_date,
--             MAX(ch.is_first_brand_purchase) as is_first_brand_purchase,
--             SUM(o.shipped_units) as customer_day_units,
--             SUM(o.revenue_share_amt) as customer_day_revenue,
--             SUM(o.display_ads_amt) as customer_day_display_ads,
--             SUM(o.subscription_revenue_amt) as customer_day_subscription_revenue
--         FROM orders_with_promos o
--             JOIN customer_history ch 
--             ON ch.customer_id = o.customer_id 
--             AND ch.customer_shipment_item_id = o.customer_shipment_item_id
--             AND ch.brand_code = o.brand_code
--             AND ch.order_date = o.order_date
--         WHERE 
--             o.promo_start_date BETWEEN 
--                 TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '365 days'
--                 AND TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
--         GROUP BY 
--             o.gl_product_group,
--             o.brand_code,
--             o.brand_name,
--             o.event_name,
--             o.promo_start_date,
--             o.promo_end_date,
--             o.event_month,
--             o.event_year,
--             o.period_type,
--             o.customer_id,
--             o.order_date
--     )
--     SELECT 
--         gl_product_group,
--         brand_code,
--         brand_name,
--         event_name,
--         promo_start_date,
--         promo_end_date,
--         event_month,
--         event_year,
--         period_type,
--         -- Performance metrics
--         SUM(customer_day_units) as total_units,
--         SUM(customer_day_revenue) as total_revenue,
--         SUM(customer_day_display_ads) as total_display_ads,
--         SUM(customer_day_subscription_revenue) as total_subscription_revenue,
--         -- Customer metrics
--         COUNT(DISTINCT customer_id) as total_customers,
--         COUNT(DISTINCT CASE 
--             WHEN is_first_brand_purchase = 1 
--             THEN customer_id 
--         END) as new_to_brand_customers,
--         -- Return customers = Total - New
--         (COUNT(DISTINCT customer_id) - 
--          COUNT(DISTINCT CASE 
--              WHEN is_first_brand_purchase = 1 
--              THEN customer_id 
--          END)) as total_return_customers
--     FROM brand_level_customers
--     GROUP BY 
--         gl_product_group,
--         brand_code,
--         brand_name,
--         event_name,
--         promo_start_date,
--         promo_end_date,
--         event_month,
--         event_year,
--         period_type
-- );

/*************************
FINAL OUTPUT 2: BRAND-LEVEL ANALYSIS (Simplified)
Aggregates metrics at the brand level
- Provides brand-level view of promotional performance
- Focuses on key metrics without detailed return periods
*************************/
-- DROP TABLE IF EXISTS pm_sandbox_aqxiao.ntb_brand;
-- CREATE TABLE pm_sandbox_aqxiao.ntb_brand AS (
--     WITH brand_vendor AS (
--         SELECT DISTINCT 
--             brand_code,
--             MAX(dama_mfg_vendor_code) as vendor_code  -- Added alias for the MAX function
--         FROM andes.BOOKER.D_MP_ASIN_MANUFACTURER
--         WHERE marketplace_id = 7
--             AND region_id = 1
--             AND brand_code IS NOT NULL
--         GROUP BY brand_code
--     )
--     SELECT 
--         -- Rest of the query remains the same
--         ea.gl_product_group,
--         bv.vendor_code,
--         ea.brand_code,
--         ea.brand_name,

--         -- Event details
--         ea.event_name,
--         ea.promo_start_date,
--         ea.promo_end_date,
--         ea.event_month,
--         ea.event_year,

--         -- Calculate event duration
--         (CASE 
--             WHEN ea.promo_end_date IS NULL THEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
--             WHEN ea.promo_end_date > TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') THEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
--             ELSE ea.promo_end_date 
--         END - ea.promo_start_date + 1) as event_duration_days,

--         -- Deal period metrics
--         SUM(CASE WHEN ea.period_type = 'DEAL' THEN ea.total_units ELSE 0 END) as deal_total_units,
--         SUM(CASE WHEN ea.period_type = 'DEAL' THEN ea.total_revenue ELSE 0 END) as deal_total_revenue,
--         SUM(CASE WHEN ea.period_type = 'DEAL' THEN ea.total_customers ELSE 0 END) as deal_total_customers,
--         SUM(CASE WHEN ea.period_type = 'DEAL' THEN ea.new_to_brand_customers ELSE 0 END) as deal_new_to_brand_customers,
--         SUM(CASE WHEN ea.period_type = 'DEAL' THEN ea.total_return_customers ELSE 0 END) as deal_return_customers,

--         -- Pre-deal period metrics
--         SUM(CASE WHEN ea.period_type = 'PRE_DEAL' THEN ea.total_units ELSE 0 END) as pre_deal_total_units,
--         SUM(CASE WHEN ea.period_type = 'PRE_DEAL' THEN ea.total_revenue ELSE 0 END) as pre_deal_total_revenue,
--         SUM(CASE WHEN ea.period_type = 'PRE_DEAL' THEN ea.total_customers ELSE 0 END) as pre_deal_total_customers,
--         SUM(CASE WHEN ea.period_type = 'PRE_DEAL' THEN ea.new_to_brand_customers ELSE 0 END) as pre_deal_new_to_brand_customers,
--         SUM(CASE WHEN ea.period_type = 'PRE_DEAL' THEN ea.total_return_customers ELSE 0 END) as pre_deal_return_customers,

--         -- Growth percentages
--         ROUND(CASE 
--             WHEN SUM(CASE WHEN ea.period_type = 'PRE_DEAL' THEN ea.new_to_brand_customers ELSE 0 END) = 0 THEN 0
--             ELSE ((SUM(CASE WHEN ea.period_type = 'DEAL' THEN ea.new_to_brand_customers ELSE 0 END)::FLOAT / 
--                 SUM(CASE WHEN ea.period_type = 'PRE_DEAL' THEN ea.new_to_brand_customers ELSE 0 END)) - 1) * 100 
--         END, 2) as new_customer_growth_pct,
--         ROUND(CASE 
--             WHEN SUM(CASE WHEN ea.period_type = 'PRE_DEAL' THEN ea.total_return_customers ELSE 0 END) = 0 THEN NULL
--             ELSE ((SUM(CASE WHEN ea.period_type = 'DEAL' THEN ea.total_return_customers ELSE 0 END)::FLOAT / 
--                 SUM(CASE WHEN ea.period_type = 'PRE_DEAL' THEN ea.total_return_customers ELSE 0 END)) - 1) * 100 
--         END, 2) as return_customer_growth_pct
    
--     FROM event_analysis_brand ea
--         LEFT JOIN deduplicated_events de 
--             ON ea.brand_code = de.brand_code 
--             AND ea.event_name = de.event_name
--             AND ea.promo_start_date = de.promo_start_date
--             AND ea.promo_end_date = de.promo_end_date
--         LEFT JOIN brand_vendor bv
--             ON ea.brand_code = bv.brand_code
--     GROUP BY 
--         ea.gl_product_group,
--         bv.vendor_code,
--         ea.brand_code,
--         ea.brand_name,
--         ea.event_name,
--         ea.promo_start_date,
--         ea.promo_end_date,        
--         ea.event_month,
--         ea.event_year
--     ORDER BY 
--         ea.promo_start_date DESC,
--         deal_total_revenue DESC
-- );



-- Grant permissions
-- GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_asin TO PUBLIC;
-- GRANT ALL ON TABLE pm_sandbox_aqxiao.ntb_brand TO PUBLIC;
