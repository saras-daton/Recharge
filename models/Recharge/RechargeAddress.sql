{% if var('RechargeAddress') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("recharge_address_tbl_ptrn",'%recharge%address',"recharge_address_tbl_exclude_ptrn",'') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        cast(a.id as string) as id,		
        cast(customer_id as string) as customer_id,		
        cast(payment_method_id as string) as payment_method_id,		
        address1,		
        address2,		
        city,		
        company,		
        country_code,		
        {{timezone_conversion("created_at")}} as created_at,
        first_name,		
        last_name,		
        phone,		
        province,		
        {{timezone_conversion("updated_at")}} as updated_at,
        zip,			
        order_note,		
        presentment_currency,		

        {{extract_nested_value("cart_attributes","name","string")}} as cart_attributes_name,
        {{extract_nested_value("cart_attributes","value","string")}} as cart_attributes_value,
        {{extract_nested_value("note_attributes","name","string")}} as note_attributes_name,
        {{extract_nested_value("note_attributes","value","string")}} as note_attributes_value,	
        cart_note,		
        country,		
        {{timezone_conversion("created_at")}} as created_at_dtm,
        {{timezone_conversion("updated_at_dtm")}} as updated_at_dtm,
        cast(discount_id as string) as discount_id,		
        {{extract_nested_value("shipping_lines_conserved","code","string")}} as shipping_lines_conserved_code,
        {{extract_nested_value("shipping_lines_conserved","price","numeric")}} as shipping_lines_conserved_price,
        {{extract_nested_value("shipping_lines_conserved","title","string")}} as shipping_lines_conserved_title,
        {{extract_nested_value("shipping_lines_conserved","id","string")}} as shipping_lines_conserved_id,
        {{extract_nested_value("shipping_lines_conserved","discounted_price","numeric")}} as shipping_lines_conserved_discounted_price,
        {{extract_nested_value("shipping_lines_conserved","source","string")}} as shipping_lines_conserved_source,

        {{extract_nested_value("shipping_lines_override","code","string")}} as shipping_lines_override_code,
        {{extract_nested_value("shipping_lines_override","price","numeric")}} as shipping_lines_override_price,
        {{extract_nested_value("shipping_lines_override","title","string")}} as shipping_lines_override_title,	
        {{extract_nested_value("shipping_lines_override","price_st","string")}} as shipping_lines_override_price_st,	
        {{extract_nested_value("shipping_lines_override","tax_lines","string")}} as shipping_lines_override_tax_lines,	

        {{extract_nested_value("original_shipping_lines","code","string")}} as original_shipping_lines_code,
        {{extract_nested_value("original_shipping_lines","price","numeric")}} as original_shipping_lines_price,
        {{extract_nested_value("original_shipping_lines","title","string")}} as original_shipping_lines_title,	    

        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
            {{unnesting("cart_attributes")}}
            {{unnesting("note_attributes")}}
            {{unnesting("shipping_lines_conserved")}}
            {{unnesting("shipping_lines_override")}}
            {{unnesting("original_shipping_lines")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('recharge_address_lookback') }},0) from {{ this }})
        {% endif %}
            qualify dense_rank() over (partition by id,payment_method_id order by {{daton_batch_runtime()}} desc) = 1
{% if not loop.last %} union all {% endif %}
{% endfor %} 
