
{% if var('RechargeOrderLineItemsProperties') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

    {% if is_incremental() %}
    {%- set max_loaded_query -%}
    SELECT coalesce(MAX({{daton_batch_runtime()}}) - 2592000000,0) FROM {{ this }}
    {% endset %}
    {%- set max_loaded_results = run_query(max_loaded_query) -%}
    {%- if execute -%}
    {% set max_loaded = max_loaded_results.rows[0].values()[0] %}
    {% else %}
    {% set max_loaded = 0 %}
    {%- endif -%}
    {% endif %}
    {% set table_name_query %}
        {{set_table_name('%recharge_bq_orders')}}    
    {% endset %} 

    {% set results = run_query(table_name_query) %}
    {% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}
    {% for i in results_list %}
        {% if var('get_brandname_from_tablename_flag') %}
            {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
        {% else %}
            {% set brand = var('default_brandname') %}
        {% endif %}

        {% if var('get_storename_from_tablename_flag') %}
            {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
        {% else %}
            {% set store = var('default_storename') %}
        {% endif %}

        {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
        {% endif %}

        select 
            '{{brand}}' as brand,
            '{{store}}' as store,
            coalesce(cast(a.id as string),'NA') as order_id,
            cast(address_id as string) address_id,
            {{extract_nested_value("charge","id","string")}} as charge_id,
            {{extract_nested_value("external_transaction_id","payment_processor","string")}} as external_transaction_id_payment_processor,
            {{extract_nested_value("charge","payment_processor_name","string")}} as charge_payment_processor_name,
            {{extract_nested_value("charge","status","string")}} as charge_status,
            {{extract_nested_value("client_details","browser_ip","string")}} as client_details_browser_ip,
            {{extract_nested_value("client_details","user_agent","string")}} as client_details_user_agent,
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
            currency,
            {{extract_nested_value("customer","id","string")}} as customer_id,
            {{extract_nested_value("customer","email","string")}} as customer_email,
            {{extract_nested_value("external_customer_id","ecommerce","string")}} as external_customer_id_ecommerce,
            {{extract_nested_value("customer","hash","string")}} as customer_hash,
            {{extract_nested_value("external_order_id","ecommerce","string")}} as external_order_id_ecommerce,
            {{extract_nested_value("external_order_name","ecommerce","string")}} as external_order_name_ecommerce,
            {{extract_nested_value("external_order_number","ecommerce","string")}} as external_order_number_ecommerce,
            is_prepaid,
            {{extract_nested_value("line_items","purchase_item_id","string")}} as line_items_purchase_item_id,
            {{extract_nested_value("line_items","external_inventory_policy","string")}} as line_items_external_inventory_policy,
            {{extract_nested_value("external_product_id","ecommerce","numeric")}} as external_product_id_ecommerce,
            {{extract_nested_value("external_variant_id","ecommerce","string")}} as external_variant_id_ecommerce,
            {{extract_nested_value("line_items","grams","string")}} as line_items_grams,
            {{extract_nested_value("images","large","string")}} as images_large,
            {{extract_nested_value("images","medium","string")}} as images_medium,
            {{extract_nested_value("images","small","string")}} as images_small,
            {{extract_nested_value("images","original","string")}} as images_original,
            {{extract_nested_value("properties","name","string")}} as properties_name,
            {{extract_nested_value("properties","value","string")}} as properties_value,
            {{extract_nested_value("line_items","purchase_item_type","string")}} as line_items_purchase_item_type,
            coalesce(({{extract_nested_value("line_items","sku","string")}}),'NA') as line_items_sku,
            {{extract_nested_value("line_items","taxable","string")}} as line_items_taxable,
            {{extract_nested_value("line_items","title","string")}} as line_items_title,
            {{extract_nested_value("line_items","unit_price_includes_tax","string")}} as line_items_unit_price_includes_tax,
            {{extract_nested_value("line_items","variant_title","string")}} as line_items_variant_title,
            note,
            processed_at,
            scheduled_at,
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
            {{extract_nested_value("shipping_lines","code","string")}} as shipping_lines_code,
            {{extract_nested_value("shipping_lines","price","numeric")}} as shipping_lines_price,
            {{extract_nested_value("shipping_lines","taxable","string")}} as shipping_lines_taxable,
            {{extract_nested_value("shipping_lines","title","string")}} as shipping_lines_title,
            {{extract_nested_value("shipping_lines","source","string")}} as shipping_lines_source,
            a.status,
            tags,
            a.taxable,
            type,
            updated_at,     
            external_cart_token,
            --order_attributes,
            error,
            {{extract_nested_value("tax_lines","price","numeric")}} as tax_lines_price,
            {{extract_nested_value("tax_lines","rate","string")}} as tax_lines_rate,
            {{extract_nested_value("tax_lines","title","string")}} as tax_lines_title,
            total_duties,
            {% if var('currency_conversion_flag') %}
                case when c.value is null then 1 else c.value end as exchange_currency_rate,
                case when c.from_currency_code is null then a.currency else c.from_currency_code end as exchange_currency_code,
            {% else %}
                cast(1 as decimal) as exchange_currency_rate,
                cast(null as string) as exchange_currency_code, 
            {% endif %}
            a.{{daton_user_id()}},
            a.{{daton_batch_runtime()}},
            a.{{daton_batch_id()}},
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        from {{i}} a
            {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
            {% endif %}
            {{unnesting("customer")}}
            {{multi_unnesting("customer","external_customer_id")}}
            {{unnesting("charge")}}
            {{multi_unnesting("charge","external_transaction_id")}}
            {{unnesting("client_details")}}
            {{unnesting("external_order_id")}}
            {{unnesting("external_order_name")}}
            {{unnesting("external_order_number")}}
            {{unnesting("line_items")}}
            {{multi_unnesting("line_items","properties")}}
            {{multi_unnesting("line_items","external_product_id")}}
            {{multi_unnesting("line_items","external_variant_id")}}
            {{multi_unnesting("line_items","images")}}
            {{unnesting("shipping_address")}}
            {{unnesting("shipping_lines")}}
            {{unnesting("tax_lines")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
        qualify ROW_NUMBER() OVER (PARTITION BY a.id, sku order by a.{{daton_batch_runtime()}} desc)=1
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
