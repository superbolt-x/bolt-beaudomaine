{{ config (
    alias = target.database + '_facebook_campaign_performance'
)}}

SELECT 
account_id,
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
initiate_checkout,
purchases,
revenue
FROM {{ ref('facebook_performance_by_campaign') }}
