{{ config (
    alias = target.database + '_blended_performance'
)}}

WITH sho_data AS (
    SELECT 
        'Shopify' as channel,
        '(not set)' as campaign_name,
        date,
        date_granularity,
        0 as spend,
        0 as clicks,
        0 as impressions,
        0 as paid_purchases,
        0 as paid_revenue, 
        new_net_customers as shopify_new_customers,
	      net_customers as shopify_total_customers,
	      net_orders as shopify_orders,
	      net_sales as shopify_sales,
	      first_order_net_sales as shopify_first_sales,
        0 as sessions,
        0 as engaged_sessions
    FROM {{ source('reporting','shopify_sales') }}
),

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}  
  
paid_data as
    (SELECT channel, campaign_id::varchar as campaign_id, campaign_name, date::date, date_granularity, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(paid_purchases),0) as paid_purchases, COALESCE(SUM(paid_revenue),0) as paid_revenue
    FROM
        (SELECT 'Meta' as channel, campaign_id::varchar as campaign_id, campaign_name, date, date_granularity, 
            spend, link_clicks as clicks, impressions, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','facebook_campaign_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, campaign_id::varchar as campaign_id, campaign_name, date, date_granularity,
            spend, clicks, impressions, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','googleads_campaign_performance') }}
        UNION ALL
        
        {%- for date_granularity in date_granularity_list %}
        SELECT 'Epsilon' as channel, '2026' as campaign_id, '2026' as campaign_name, 
            date_trunc('{{date_granularity}}',date) as date,
            '{{date_granularity}}' as date_granularity,
            sum(spend) as spend, sum(0) as clicks, sum(0) as impressions, sum(0) as paid_purchases, sum(0) as paid_revenue
        FROM {{ source('gsheet_raw','epsilon_spend') }}
        GROUP BY 1,2,3,4,5
        {%- if not loop.last %}UNION ALL{%- endif %}
        {% endfor %}
        )
    GROUP BY channel, campaign_id, campaign_name, date, date_granularity),

ga4_data as 
    (SELECT case 
              when source_medium = 'epsilon / display' then '2026' 
			  when source_medium in ('ig / cpc','fb / cpc','th / cpc') then split_part(campaign_id,'_',1)::varchar
			  else campaign_id::varchar 
            end as campaign_id, date, date_granularity, 
    sum(sessions) as sessions, sum(engaged_sessions) as engaged_sessions
    FROM {{ source('reporting','ga4_campaign_performance_session') }}
    GROUP BY 1,2,3),

paid_ga4_data as (
  SELECT 
    case when campaign_name is null then 'Not Paid' else channel end as channel, campaign_name, date::date, date_granularity,
    SUM(COALESCE(spend, 0)) AS spend,
    SUM(COALESCE(clicks, 0)) AS clicks,
    SUM(COALESCE(impressions, 0)) AS impressions,
    SUM(COALESCE(paid_purchases, 0)) AS paid_purchases,
    SUM(COALESCE(paid_revenue, 0)) AS paid_revenue,
    SUM(0) AS shopify_new_customers,
    SUM(0) AS shopify_total_customers,
    SUM(0) AS shopify_orders,
    SUM(0) AS shopify_sales,
    SUM(0) AS shopify_first_sales,
    SUM(COALESCE(sessions, 0)) AS sessions,
    SUM(COALESCE(engaged_sessions, 0)) AS engaged_sessions
  FROM paid_data FULL OUTER JOIN ga4_data USING(date,date_granularity,campaign_id)
  GROUP BY 1,2,3,4)

SELECT 
    channel,
    campaign_name,
    date,
    date_granularity,
    spend,
    clicks,
    impressions,
    paid_purchases,
    paid_revenue,
    shopify_new_customers,
    shopify_total_customers,
    shopify_orders,
    shopify_sales,
    shopify_first_sales,
    sessions,
    engaged_sessions
FROM (
    SELECT * FROM paid_ga4_data
    UNION ALL 
    SELECT * FROM sho_data
)
