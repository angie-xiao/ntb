/*+ETLM {
	depend:{
		replace:[
			{name:"andes.booker.D_MP_ASIN_MANUFACTURER", age:{days:1}},
			{name:"andes.booker.D_UNIFIED_CUSTOMER_ORDER_ITEMS", age:{days:1}}
			{name:"andes.contribution_ddl.O_WBR_CP_NA", age:{days:1}}
		]
	}
}*/




DROP TABLE IF EXISTS orders;
CREATE TEMP TABLE orders AS (
    SELECT DISTINCT  
    	customer_shipment_item_id, 
    	asin, 
    	customer_id, 
    	ship_day
    FROM andes.booker.D_UNIFIED_CUST_SHIPMENT_ITEMS o
    WHERE o.region_id=1
        and o.marketplace_id = 7
        and o.is_retail_merchant = 'Y'
   		and o.ship_day BETWEEN TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD') - interval '120 days' and TO_DATE('{RUN_DATE_YYYY-MM-DD}', 'YYYY-MM-DD')
        and o.order_condition != 6
);



DROP TABLE IF EXISTS orders_manu;
CREATE TEMP TABLE orders_manu AS (
    SELECT
        o.customer_shipment_item_id,
    	o.asin,
        o.customer_id,
    	o.ship_day,
        m.dama_mfg_vendor_code,
        m.dama_mfg_vendor_name,
        m.brand_name,
        m.brand_code
    FROM orders o
        INNER JOIN andes.booker.d_mp_asin_manufacturer m
        ON o.asin = m.asin
   WHERE m.region_id = 1 AND m.marketplace_id=7
);




DROP TABLE IF EXISTS order_metrics;
CREATE TEMP TABLE order_metrics AS (
    SELECT
        o.asin,
        o.customer_shipment_item_id,
        o.customer_id,
        o.ship_day,
        dama_mfg_vendor_code,
        dama_mfg_vendor_name,
        brand_name,
        brand_code,
        -- cp.is_sns,
        -- cp.prime_member_type,
        cp.revenue_share_amt,
        cp.display_ads_amt,
        cp.subscription_revenue_amt
    FROM orders_manu o
        left join andes.contribution_ddl.O_WBR_CP_NA cp     
        ON o.ship_day = cp.ship_day
        AND o.customer_shipment_item_id = cp.customer_shipment_item_id 
        AND o.asin = cp.asin 
    WHERE cp.marketplace_id = 7 
        AND cp.ship_day BETWEEN TO_DATE(cp.ship_day, 'YYYY-MM-DD') - interval '120 days' and TO_DATE(cp.ship_day, 'YYYY-MM-DD')
		AND cp.marketplace_id=7
);


DROP TABLE IF EXISTS cte1;
CREATE TEMP TABLE cte1 AS (
    SELECT
        dama_mfg_vendor_code,
        dama_mfg_vendor_name,
        brand_code,
        brand_name,
        asin,
        customer_id,
        ship_day,
        revenue_share_amt,
        display_ads_amt,
        subscription_revenue_amt,
        LAG(ship_day) OVER (PARTITION BY dama_mfg_vendor_code, customer_id ORDER BY ship_day) AS last_purchase_date,
        LAG(ASIN) OVER (PARTITION BY dama_mfg_vendor_code, customer_id ORDER BY ship_day) AS last_purchase_asin
    FROM order_metrics
    WHERE dama_mfg_vendor_code != 'NaN'
);



DROP TABLE IF EXISTS cte2;
CREATE TEMP TABLE cte2 AS (
    SELECT
        asin,
        dama_mfg_vendor_code,
        dama_mfg_vendor_name,
        brand_code,
        brand_name,
        customer_id,
        ship_day,
        -- is_sns,
        -- prime_member_type,
        revenue_share_amt,
        display_ads_amt,
        subscription_revenue_amt,
        last_purchase_asin,
        last_purchase_date,
        ( 
            CASE
                WHEN last_purchase_date IS NULL THEN 'first purchase'
                WHEN TO_DATE(last_purchase_asin, 'YYYY-MM-DD') BETWEEN TO_DATE(ship_day,'YYYY-MM-DD') - interval '30 days' AND TO_DATE(ship_day,'YYYY-MM-DD') - interval '1 days' THEN 'return in 1 mo'
                WHEN TO_DATE(last_purchase_asin,'YYYY-MM-DD') BETWEEN TO_DATE(ship_day,'YYYY-MM-DD') - interval '60 days' AND TO_DATE(ship_day,'YYYY-MM-DD')- interval '31 days' THEN 'return in 2 mo'
                WHEN TO_DATE(last_purchase_asin,'YYYYMMDD') BETWEEN TO_DATE(ship_day,'YYYY-MM-DD')  - interval '90 days' AND TO_DATE(ship_day, 'YYYY-MM-DD') -  interval '61 days' THEN 'return in 3 mo'
                WHEN TO_DATE(ship_day,'YYYY-MM-DD') - TO_DATE(last_purchase_date,'YYYY-MM-DD') > interval '90 days' THEN 'return after 3 mo+'
                ELSE '/'
            END
        ) AS last_purchase_n_days_ago
    FROM  cte1
);


GRANT ALL ON TABLE caism.new_to_brand_job_test2 TO PUBLIC;
DROP TABLE IF EXISTS CAISM.new_to_brand_job_test2;
CREATE TABLE CAISM.new_to_brand_job_test2 AS (
    SELECT
        asin,
        dama_mfg_vendor_code,
        dama_mfg_vendor_name,
        brand_name,
        brand_code,
        customer_id,
        ship_day,
        last_purchase_asin,
        last_purchase_date,
        last_purchase_n_days_ago,
        COUNT(DISTINCT customer_id) AS unique_customer_ct,
        SUM(revenue_share_amt),
        SUM(display_ads_amt),
        SUM(subscription_revenue_amt)
    FROM cte2
    GROUP BY 
        dama_mfg_vendor_code,
        asin,
        last_purchase_n_days_ago,
        ship_day,
        last_purchase_asin,
        last_purchase_date,
        -- is_sns,
        -- prime_member_type,
        brand_code,
        brand_name
        -- category
    ORDER BY
        dama_mfg_vendor_code,
        asin,
        last_purchase_n_days_ago ASC
);

