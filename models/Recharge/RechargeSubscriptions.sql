{% if var('RechargeSubscriptions') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("recharge_subscriptions_tbl_ptrn",'%recharge%subscriptions',"recharge_subscriptions_tbl_exclude_ptrn",'') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,

            cast(id as string) as subscription_id,
            cast(address_id as string) as address_id,
            cast(customer_id as string) as customer_id,
            {{timezone_conversion("a.created_at")}} as created_at,
            {{extract_nested_value("utm_params","utm_data_source","string")}} as utm_params_utm_data_source,
            {{extract_nested_value("utm_params","utm_timestamp","datetime")}} as utm_params_utm_timestamp,
            {{extract_nested_value("utm_params","utm_campaign","string")}} as utm_params_utm_campaign,
            {{extract_nested_value("utm_params","utm_content","string")}} as utm_params_utm_content,
            {{extract_nested_value("utm_params","utm_source","string")}} as utm_params_utm_source,
            {{extract_nested_value("utm_params","utm_medium","string")}} as utm_params_utm_medium,	
            {{extract_nested_value("utm_params","utm_term","string")}} as utm_params_utm_term,
            
            charge_interval_frequency,
          
            {{extract_nested_value("external_product_id","ecommerce","string")}} as external_product_id,
            {{extract_nested_value("external_variant_id","ecommerce","string")}} as external_variant_id,
            
            has_queued_charges,
            is_prepaid,
            is_skippable,
            is_swappable,
            max_retries_reached,
            next_charge_scheduled_at,
            order_interval_frequency,
            order_interval_unit,
            price,
            product_title,
            --properties,
            quantity,
            sku,
            sku_override,
            status,
            {{timezone_conversion("a.updated_at")}} as updated_at,
            variant_title,
            cancellation_reason,
            {{timezone_conversion("a.cancelled_at")}} as cancelled_at,
            order_day_of_month,
            presentment_currency,
            cancellation_reason_comments,
	        a.{{daton_user_id()}},
            a.{{daton_batch_runtime()}},
            a.{{daton_batch_id()}},
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
           
            from {{i}} a
                {{unnesting("analytics_data")}}
                {{multi_unnesting("analytics_data","utm_params")}}
                {{unnesting("external_product_id")}}
                {{unnesting("external_variant_id")}}    
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                

     where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('recharge_subscriptions_lookback') }},0) from {{ this }})
        {% endif %}
            qualify  row_number() over (partition by a.id,external_product_id,external_variant_id,sku order by a.{{daton_batch_runtime()}} desc, next_charge_scheduled_at desc) =1
{% if not loop.last %} union all {% endif %}
{% endfor %} 
