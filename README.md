# Recharge Data Unification

This dbt package is for the Recharge Ads data unification Ingested by [Daton](https://sarasanalytics.com/daton/). [Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

### Supported Datawarehouses:
- BigQuery

#### Typical challanges with raw data are:
- Array/Nested Array columns which makes queries for Data Analytics complex
- Data duplication due to look back period while fetching report data from Recharge
- Seperate tables at marketplaces/Store, brand, account level for same kind of report/data feeds

By doing Data Unification the above challenges can be overcomed and simplifies Data Analytics. 
As part of Data Unification, the following funtions are performed:
- Consolidation - Different marketplaces/Store/account & different brands would have similar raw Daton Ingested tables, which are consolidated into one table with column distinguishers brand & store
- Deduplication - Based on primary keys, the data is De-duplicated and the latest records are only loaded into the consolidated stage tables
- Incremental Load - Models are designed to include incremental load which when scheduled would update the tables regularly
- Standardization -
	- Currency Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local currency of the corresponding marketplace/store/account. Values that are in local currency are standardized by converting to desired currency using Daton Exchange Rates data.
	  Prerequisite - Exchange Rates connector in Daton needs to be present - Refer [this](https://github.com/saras-daton/currency_exchange_rates)
	- Time Zone Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local timezone of the corresponding marketplace/store/account. DateTime values that are in local timezone are standardized by converting to specified timezone using input offset hours.

#### Prerequisite 
Daton Integrations for  
- Recharge
- Exchange Rates(Optional, if currency conversion is not required)

# Installation & Configuration

## Installation Instructions

If you haven't already, you will need to create a packages.yml file in your DBT project. Include this in your `packages.yml` file

```yaml
packages:
  - package: saras-daton/Recharge
    version: 1.0.0
```

# Configuration 

## Required Variables

This package assumes that you have an existing dbt project with a BigQuery profile connected & tested. Source data is located using the following variables which must be set in your `dbt_project.yml` file.
```yaml
vars:
    raw_database: "your_database"
    raw_schema: "your_schema"
```

## Setting Target Schema

Models will be create unified tables under the schema (<target_schema>_stg_Recharge). In case, you would like the models to be written to the target schema or a different custom schema, please add the following in the dbt_project.yml file.

```yaml
models:
  Recharge:
    +schema: custom_schema_name
```

## Optional Variables

Package offers different configurations which must be set in your `dbt_project.yml` file. These variables can be marked as True/False based on your requirements. Details about the variables are given below.

### Currency Conversion 

To enable currency conversion, which produces two columns - exchange_currency_rate & exchange_currency_code, please mark the currency_conversion_flag as True. By default, it is False.
Prerequisite - Daton Exchange Rates Integration

Example:
```yaml
vars:
    currency_conversion_flag: True
```

### Timezone Conversion 

To enable timezone conversion, which converts the timezone columns from UTC timezone to local timezone, please mark the timezone_conversion_flag as True in the dbt_project.yml file, by default, it is FalseAdditionally, you need to provide offset hours between UTC and the timezone you want the data to convert into for each raw table

Example:
```yaml
vars:
  timezone_conversion_flag : True
  raw_table_timezone_offset_hours: {
    "edm-saras.EDM_Daton.Brand_US_Recharge_BQ_Charges" : -7,
    "edm-saras.EDM_Daton.Brand_US_Recharge_BQ_Customers" : -7,
    "edm-saras.EDM_Daton.Brand_US_Recharge_BQ_Discounts" : -7,
    "edm-saras.EDM_Daton.Brand_US_Recharge_BQ_Onetimes" : -7,
    "edm-saras.EDM_Daton.Brand_US_Recharge_BQ_Address" : -7,
    "edm-saras.EDM_Daton.Brand_US_Recharge_BQ_Products" : -7,
    "edm-saras.EDM_Daton.Brand_US_Recharge_BQ_Collections" : -7,
    "edm-saras.EDM_Daton.Brand_US_Recharge_BQ_Orders" : -7,
    "edm-saras.EDM_Daton.Brand_US_Recharge_BQ_Subscriptions" : -7
    }

```
Here, -7 represents the offset hours between UTC and PDT considering we are sitting in PDT timezone and want the data in this timezone


### Table Exclusions

If you need to exclude any of the models, declare the model names as variables and mark them as False. Refer the table below for model details. By default, all tables are created.

Example:
```yaml
vars:
RechargeAddress: False
```

## Models

This package contains models from the Recharge API which includes reports on {{sales, margin, inventory, product}}. The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** |
| ------------------------- | ---------------| ----------------------- |
|Address | [RechargeAddress](models/Recharge/RechargeAddress.sql)  | This table provides courier details for a reverse pick-up (CIR) for a returned order item in Uniware. |
|Charges | [RechargeCharges](models/Recharge/RechargeCharges.sql)  | This table lists all the Gatepass details . A Gatepass is a simple document containing the detail of items while making any product movement outside the warehouse |
|Collections | [RechargeCollections](models/Recharge/RechargeCollections.sql)  | This table lists all the issued GRN with item details for a vendor compared to Purchase Order.A GRN (Goods Received Note) is a record used to confirm all goods have been received|
|Customers | [RechargeCustomers](models/Recharge/RechargeCustomers.sql)  | This table provides complete overview of inventory distribution of a SKU in a facility and inventory overview of SKU(s)) based on time since last inventory update |
|Discounts | [RechargeDiscounts](models/Recharge/RechargeDiscounts.sql)  | This table contains a list of invoices generated with detailed invoice sections and shipping code based on order number|
|Onetimes | [RechargeOnetimes](models/Recharge/RechargeOnetimes.sql)  | The list of products or items existing within Uniware. It defines the product SKU code (which is unique for each item), its name, the product category it belongs to and the tax associated if any |
|Orders | [RechargeOrderLineItems](models/Recharge/RechargeOrderLineItems.sql)  | A list purchase orders for products in inventory along with its status [Purchase Order is a document shared with the vendor indicating the product (SKUs), their respective quantities and the agreed price] |
|Orders | [RechargeOrderLineItemsProperties](models/Recharge/RechargeOrderLineItemsProperties.sql)  | This table provides putaway item details based on Inventory Shelf code |
|Products | [RechargeProducts](models/Recharge/RechargeProducts.sql)  | This table lists all the return invioces genrated with the shipping details based on item sku code  |
|Subscriptions | [RechargeSubscriptions](models/Recharge/RechargeSubscriptions.sql)  | This table provides reverse pickup details with sale order item code and putaway details if applicable |


### For details about default configurations for Table Primary Key columns, Partition columns, Clustering columns, please refer the properties.yaml used for this package as below. 
	You can overwrite these default configurations by using your project specific properties yaml.
```yaml
version: 2
models:
  - name: RechargeAddress
    description: This table provides address details for a customer based on a customer id, Each customer can have multiple addresses.
    config:
        materialized : incremental
        incremental_strategy : merge
        partition_by : { 'field': 'created_at', 'data_type': 'timestamp' }
        cluster_by : ['id'] 
        unique_key : ['id','payment_method_id']

  - name: RechargeCharges
    description: A charge is the representation of a financial transaction linked to the purchase of an item. A Charge is linked to its corresponding orders.
    config:
        materialized : incremental
        incremental_strategy : merge
        partition_by : { 'field': 'created_at', 'data_type': 'timestamp' }
        cluster_by : ['id'] 
        unique_key : ['id','line_items_sku','line_items_subscription_id']

  - name: RechargeCollections
    description: This table contains all the collections , contains an ordered list of Products and can be used for selective display of Products on chosen interfaces 
    config:
        materialized : incremental
        incremental_strategy : merge
        partition_by : { 'field': 'created_at', 'data_type': 'timestamp' }
        cluster_by : ['id'] 
        unique_key : ['id']

  - name: RechargeCustomers
    description: This table provides the account information. Email is unique on the Customer; no two customers for a store can have the same email
    config:
        materialized : incremental
        incremental_strategy : merge
        partition_by : { 'field': 'created_at', 'data_type': 'timestamp' }
        cluster_by : ['id'] 
        unique_key : ['id','email']

  - name: RechargeDiscounts
    description: This table contains a list of Discounts thats applied to a Checkout, or can be applied directly to an Address
    config:
        materialized : incremental
        incremental_strategy : merge
        partition_by : { 'field': 'created_at', 'data_type': 'timestamp' }
        cluster_by : ['id'] 
        unique_key : ['id']

  - name: RechargeOnetimes
    description: The table Returns a list of all Onetime products from store.
    config:
        materialized : incremental
        incremental_strategy : merge
        partition_by : { 'field': 'created_at', 'data_type': 'timestamp' }
        cluster_by : ['id'] 
        unique_key : ['id','customer_id','address_id']


  - name: RechargeOrderLineItems
    description: This Table provides details of all orders created after a Charge is successfully processed at line items granularity.
    config:
        materialized : incremental
        incremental_strategy : merge
        partition_by : { 'field': 'created_at', 'data_type': 'date' }
        cluster_by : ['order_id'] 
        unique_key : ['order_id','sku']

  - name: RechargeOrderLineItemProperties
    description:  This Table provides details of all orders created after a Charge is successfully processed at line items granularity with properties.
    config:
        materialized : incremental
        incremental_strategy : merge
        partition_by : { 'field': 'created_at', 'data_type': 'date' }
        cluster_by : ['order_id'] 
        unique_key : ['order_id','sku','name']

  - name: RechargeProducts
    description: This table lists all the product records in recharge.
    config:
        materialized : incremental
        incremental_strategy : merge
        partition_by : { 'field': 'created_at', 'data_type': 'timestamp' }
        cluster_by : ['id'] 
        unique_key : ['id']


  - name: RechargeSubscriptions
    description: This table provides details of subscription based on customer id.
    config:
        materialized : incremental
        incremental_strategy : merge
        partition_by : { 'field': 'updated_at', 'data_type': 'date' }
        cluster_by : ['subscription_id'] 
        unique_key : ['subscription_id','external_product_id','external_variant_id','sku']

```
## Resources:
- Have questions, feedback, or need [help](https://calendly.com/priyanka-vankadaru/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a dbt account & connect to {{Bigquery}}