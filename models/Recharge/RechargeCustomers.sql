
{% if var('RechargeCustomers') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}
{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("recharge_customers_tbl_ptrn",'%recharge%customers',"recharge_customers_tbl_exclude_ptrn",'') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        cast(a.id as string) as id	,	
        {{timezone_conversion("created_at")}} as created_at,
        email	,		
        {{extract_nested_value("external_customer_id","ecommerce","string")}} as external_customer_id_ecommerce,
        {{timezone_conversion("first_charge_processed_at")}} as first_charge_processed_at,	
        first_name	,		
        has_payment_method_in_dunning	,		
        has_valid_payment_method,		
        a.hash,
        last_name	,		
        subscriptions_active_count	,		
        subscriptions_total_count	,		
        {{timezone_conversion("updated_at")}} as updated_at,
        tax_exempt	,		
        phone	,		
        {{extract_nested_value("utm_params","utm_data_source","string")}} as utm_params_utm_data_source,
        {{extract_nested_value("utm_params","utm_source","string")}} as utm_params_utm_source,
        {{extract_nested_value("utm_params","utm_timestamp","datetime")}} as utm_params_utm_timestamp,
        {{extract_nested_value("utm_params","utm_campaign","string")}} as utm_params_utm_campaign,
        {{extract_nested_value("utm_params","utm_content","string")}} as utm_params_utm_content,
        {{extract_nested_value("utm_params","utm_medium","string")}} as utm_params_utm_medium,
        {{extract_nested_value("utm_params","utm_term","string")}} as utm_params_utm_term,
        accepts_marketing	,		
        billing_address1	,		
        billing_city	,		
        billing_country	,		
        billing_phone	,		
        billing_province	,		
        billing_zip	,		
        has_card_error_in_dunning	,		
        number_active_subscriptions	,		
        number_subscriptions	,		
        processor_type	,		
        shopify_customer_id	,		
        status	,		
        stripe_customer_token	,		
        created_at_dtm	,		
        first_charge_processed_at_dtm	,		
        updated_at_dtm	,		
        braintree_customer_token	,		
        billing_address2	,		
        reason_payment_method_not_valid	,		
        billing_company	,		
        paypal_customer_token	,		
        apply_credit_to_next_recurring_charge,				
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        from {{i}} a
            {{unnesting("external_customer_id")}}
            {{unnesting("analytics_data")}}

            {{multi_unnesting("analytics_data","utm_params")}}

            {% if is_incremental() %}
           
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('recharge_customers_lookback') }},0) from {{ this }})
        {% endif %}
            qualify dense_rank() over (partition by a.id, email order by {{daton_batch_runtime()}} desc) = 1
{% if not loop.last %} union all {% endif %}
{% endfor %} 
