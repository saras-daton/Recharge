    {% if var('RechargeCharges') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}
{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('recharge_charges_tbl_ptrn'),
exclude=var('recharge_charges_tbl_exclude_ptrn'),
database=var('raw_database')) %}

{% for i in relations %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =replace(i,'`','').split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =replace(i,'`','').split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

 {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
        {% endif %}

    /*SELECT * {{exclude()}} (row_num)
    From (*/
        select
        '{{brand}}' as brand,
        '{{store}}' as store,
        coalesce(cast(a.id as string),'NA') as id,		
        cast(address_id as string) as address_id,		
        /*billing_address,*/
        {{extract_nested_value("billing_address","address1","string")}} as billing_address_address1,
        {{extract_nested_value("billing_address","city","string")}} as billing_address_city,
        {{extract_nested_value("billing_address","country_code","string")}} as billing_address_country_code,
        {{extract_nested_value("billing_address","first_name","string")}} as billing_address_first_name,
        {{extract_nested_value("billing_address","last_name","string")}} as billing_address_last_name,
        {{extract_nested_value("billing_address","phone","string")}} as billing_address_phone,
        {{extract_nested_value("billing_address","province","string")}} as billing_address_province,
        {{extract_nested_value("billing_address","zip","string")}} as billing_address_zip,
        {{extract_nested_value("billing_address","address2","string")}} as billing_address_address2,
        {{extract_nested_value("billing_address","company","string")}} as billing_address_company,
        {{extract_nested_value("billing_address","country","string")}} as billing_address_country,
        /*client_details,*/
        {{extract_nested_value("client_details","browser_ip","string")}} as client_details_browser_ip,
        {{extract_nested_value("client_details","user_agent","string")}} as client_details_user_agent,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,	
        currency,	
        --customer,	
        /*{{extract_nested_value("customer","id","string")}} as customer_id,*/
        {{extract_nested_value("customer","email","string")}} as customer_email,
        {{extract_nested_value("external_customer_id","ecommerce","string")}} as external_customer_id,
        /*external_order_id,	*/
        {{extract_nested_value("external_order_id","ecommerce","string")}} as external_order_id_ecommerce,
        /*external_transaction_id,*/		
        {{extract_nested_value("external_transaction_id","payment_processor","string")}} as external_transaction_id_payment_processor,
        has_uncommitted_changes,
        --line_items.purchase_item_id	as line_items_purchase_item_id,
        {{extract_nested_value("line_items","purchase_item_id","string")}} as line_items_purchase_item_id,
        -- line_items.external_product_id as line_items_external_product_id,	
        -- line_items.external_variant_id as line_items_external_variant_id,			
        {{extract_nested_value("external_product_id","ecommerce","string")}} as external_product_id_ecommerce,
        {{extract_nested_value("external_variant_id","ecommerce","string")}} as external_variant_id_ecommerce,
        -- line_items.grams as line_items_grams,		
        {{extract_nested_value("line_items","grams","numeric")}} as line_items_grams,
        --line_items.images as line_items_images,			
        {{extract_nested_value("images","large","string")}} as images_large,
        {{extract_nested_value("images","medium","string")}} as images_medium,
        {{extract_nested_value("images","small","string")}} as images_small,
        {{extract_nested_value("images","original","string")}} as images_original,
        --line_items.properties as line_items_properties,	
        -- line_items.purchase_item_type as line_items_purchase_item_type,
        {{extract_nested_value("line_items","purchase_item_type","string")}} as line_items_purchase_item_type,
        -- line_items.quantity as line_items_quantity,	
        {{extract_nested_value("line_items","quantity","int64")}} as line_items_quantity,
        -- line_items.sku as line_items_sku,		
        coalesce(({{extract_nested_value("line_items","sku","string")}}),'NA') as line_items_sku,
        -- line_items.tax_due as line_items_tax_due,		
        {{extract_nested_value("line_items","tax_due","string")}} as line_items_tax_due,
        -- line_items.taxable as line_items_taxable,		
        {{extract_nested_value("line_items","taxable","boolean")}} as line_items_taxable,
        -- line_items.taxable_amount as line_items_taxable_amount,		
        {{extract_nested_value("line_items","taxable_amount","numeric")}} as line_items_taxable_amount,
        -- line_items.title as line_items_title,		
        {{extract_nested_value("line_items","title","string")}} as line_items_title,
        -- line_items.total_price as line_items_total_price,		
        {{extract_nested_value("line_items","total_price","numeric")}} as line_items_total_price,
        -- line_items.unit_price as line_items_unit_price,
        {{extract_nested_value("line_items","unit_price","numeric")}} as line_items_unit_price,
        -- line_items.unit_price_includes_tax as line_items_unit_price_includes_tax,		
        {{extract_nested_value("line_items","unit_price_includes_tax","string")}} as line_items_unit_price_includes_tax,
        -- line_items.variant_title as line_items_variant_title,		
        {{extract_nested_value("line_items","variant_title","string")}} as line_items_variant_title,
        -- line_items.original_price as line_items_original_price,		
        {{extract_nested_value("line_items","original_price","numeric")}} as line_items_original_price,
        -- line_items.price as line_items_price,		
        {{extract_nested_value("line_items","price","numeric")}} as line_items_price,
        -- line_items.shopify_product_id as line_items_shopify_product_id,		
        {{extract_nested_value("line_items","shopify_product_id","string")}} as line_items_shopify_product_id,
        -- line_items.shopify_variant_id as line_items_shopify_variant_id,		
        {{extract_nested_value("line_items","shopify_variant_id","string")}} as line_items_shopify_variant_id,
        -- line_items.subscription_id as line_items_subscription_id,		
        coalesce(({{extract_nested_value("line_items","subscription_id","string")}}),'NA') as line_items_subscription_id,
        -- line_items.type as line_items_type,		
        {{extract_nested_value("line_items","type","string")}} as line_items_type,
        -- line_items.vendor as line_items_vendor,		
        {{extract_nested_value("line_items","vendor","string")}} as line_items_vendor,
        -- line_items.shopify_product_id_st as line_items_shopify_product_id_st,	
        {{extract_nested_value("line_items","shopify_product_id_st","string")}} as line_items_shopify_product_id_st,
        -- line_items.shopify_variant_id_st as line_items_shopify_variant_id_st,
        {{extract_nested_value("line_items","shopify_variant_id_st","string")}} as line_items_shopify_variant_id_st,
        note,		
        --order_attributes,		
        orders_count,		
        a.payment_processor,		
        processed_at,		
        scheduled_at,		
        /*shipping_address,*/
        {{extract_nested_value("shipping_address","address1","string")}} as shipping_address_address1,
        {{extract_nested_value("shipping_address","city","string")}} as shipping_address_city,
        {{extract_nested_value("shipping_address","country_code","string")}} as shipping_address_country_code,
        {{extract_nested_value("shipping_address","first_name","string")}} as shipping_address_first_name,
        {{extract_nested_value("shipping_address","last_name","string")}} as shipping_address_last_name,
        {{extract_nested_value("shipping_address","phone","string")}} as shipping_address_phone,
        {{extract_nested_value("shipping_address","province","string")}} as shipping_address_province,
        {{extract_nested_value("shipping_address","zip","string")}} as shipping_address_zip,
        {{extract_nested_value("shipping_address","address2","string")}} as shipping_address_address2,
        {{extract_nested_value("shipping_address","company","string")}} as shipping_address_company,
        {{extract_nested_value("shipping_address","country","string")}} as shipping_address_country,
        -- shipping_lines,	
        {{extract_nested_value("shipping_lines","code","string")}} as shipping_lines_code,
        {{extract_nested_value("shipping_lines","price","numeric")}} as shipping_lines_price,
        {{extract_nested_value("shipping_lines","taxable","bool")}} as shipping_lines_taxable,
        {{extract_nested_value("shipping_lines","title","string")}} as shipping_lines_title,
        {{extract_nested_value("shipping_lines","source","string")}} as shipping_lines_source,	
        a.status,		
        cast(a.subtotal_price as numeric) as subtotal_price,		
        tags,		
        a.taxable,		
        cast(a.total_discounts as numeric) as total_discounts,		
        cast(total_line_items_price as numeric) total_line_items_price,		
        cast(a.total_price as numeric) as total_price,		
        cast(total_refunds as numeric) as total_refunds,		
        a.total_tax,		
        total_weight_grams,		
        a.type,		
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,		
        --discounts,
        {{extract_nested_value("discounts","id","string")}} as discounts_id,
        {{extract_nested_value("discounts","code","string")}} as discounts_code,
        {{extract_nested_value("discounts","value","numeric")}} as discounts_value,
        {{extract_nested_value("discounts","value_type","string")}} as discounts_value_type,
       -- analytics_data,
        {{extract_nested_value("utm_params","utm_data_source","string")}} as utm_params_utm_data_source,
        {{extract_nested_value("utm_params","utm_timestamp","datetime")}} as utm_params_utm_timestamp,
        {{extract_nested_value("utm_params","utm_campaign","string")}} as utm_params_utm_campaign,
        {{extract_nested_value("utm_params","utm_content","string")}} as utm_params_utm_content,
        {{extract_nested_value("utm_params","utm_source","string")}} as utm_params_utm_source,
        {{extract_nested_value("utm_params","utm_medium","string")}} as utm_params_utm_medium,	
        {{extract_nested_value("utm_params","utm_term","string")}} as utm_params_utm_term,
        error,		
        error_type,		
        retry_date,		
        charge_attempts,		
        last_charge_attempt,		
        --note_attributes,
        {{extract_nested_value("note_attributes","name","string")}} as note_attributes_name,
        {{extract_nested_value("note_attributes","value","string")}} as note_attributes_value,		
        customer_hash,		
        cast(customer_id as string) as customer_id,		
        a.email,		
        a.first_name,		
        has_uncommited_changes,		
        a.last_name,		
        processor_name,		
        requires_shipping,		
        shipments_count,		
        shopify_order_id,		
        a.tax_lines,		
        total_weight,		
        transaction_id,		
        created_at_dtm,	    
        processed_at_dtm,		
        scheduled_at_dtm,		
        updated_at_dtm,		
        --discount_codes,	
        {{extract_nested_value("discount_codes","amount","numeric")}} as discount_codes_amount,
        {{extract_nested_value("discount_codes","code","string")}} as discount_codes_code,
        {{extract_nested_value("discount_codes","recharge_discount_id","string")}} as discount_codes_recharge_discount_id,
        {{extract_nested_value("discount_codes","type","string")}} as discount_codes_type,
        last_charge_attempt_date,		
        number_times_tried,		
        external_variant_not_found,		
        total_duties,		
        retry_date_dtm,		
        shopify_variant_id_not_found,
        taxes_included,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then a.currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            a.currency as exchange_currency_code, 
        {% endif %}					
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        /*dense_rank() over (partition by a.id order by a.{{daton_batch_runtime()}} desc) row_num*/
        from {{i}} a
            {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
            {% endif %}
            {{unnesting("line_items")}}
            {{unnesting("billing_address")}}
            {{unnesting("shipping_address")}}
            {{unnesting("external_order_id")}}
            {{unnesting("external_transaction_id")}}
            {{unnesting("client_details")}}
            {{unnesting("customer")}}
            {{multi_unnesting("customer","external_customer_id")}}
            {{multi_unnesting("line_items","external_product_id")}}
            {{multi_unnesting("line_items","external_variant_id")}}
            {{multi_unnesting("line_items","images")}}
            {{unnesting("shipping_lines")}}
            {{unnesting("discounts")}}
            {{unnesting("analytics_data")}}
            {{multi_unnesting("analytics_data","utm_params")}}
            {{unnesting("note_attributes")}}
            {{unnesting("discount_codes")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
     
    where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('recharge_charges_lookback') }},0) from {{ this }})
        {% endif %}
            qualify row_number() over (partition by a.id, line_items_subscription_id, line_items_sku   order by a.{{daton_batch_runtime()}} desc)=1
{% if not loop.last %} union all {% endif %}
{% endfor %} 
