# Databricks DLT Data Engineering Project

## 📌 Project Overview

This project demonstrates an end-to-end **data engineering pipeline using Databricks, PySpark, Delta Live Tables (DLT), Auto Loader, Delta Lake, and SCD Type 2**.

The pipeline follows the **Medallion Architecture**:

```text
                    Source CSV Files
                           │
                           ▼
                    AWS S3 / Cloud Storage
                           │
                           ▼
                  ┌───────────────────┐
                  │      BRONZE       │
                  │   Auto Loader     │
                  │   Raw Streaming   │
                  └─────────┬─────────┘
                            │
                            ▼
                  ┌───────────────────┐
                  │      SILVER       │
                  │ Data Cleansing    │
                  │ Standardization   │
                  │ Data Quality      │
                  └─────────┬─────────┘
                            │
                            ▼
                  ┌───────────────────┐
                  │       GOLD        │
                  │  SCD Type 2       │
                  │  Dimensions       │
                  │  Fact Table       │
                  └───────────────────┘
````

The project processes Northwind-style business data and creates analytical dimension and fact tables.

---

## 🛠️ Technologies Used

* Databricks
* PySpark
* Delta Live Tables (DLT)
* Spark Structured Streaming
* Auto Loader
* Delta Lake
* Python
* SQL
* AWS S3
* Medallion Architecture
* Data Quality Expectations
* Change Data Capture (CDC)
* SCD Type 2
* Materialized Views

---

## 🏗️ Architecture

The pipeline consists of three main layers:

### 🥉 Bronze Layer

The Bronze layer ingests raw CSV files from cloud storage using **Databricks Auto Loader**.

The project uses:

```python
spark.readStream.format("cloudFiles")
```

and reads the source datasets from cloud storage.

The Bronze layer contains:

* Customers
* Employees
* Products
* Categories
* Orders
* Order Details
* Shippers
* Suppliers
* Shipments

Each dataset is created as a DLT table.

Example:

```python
@dp.table(
    name="dlt_project.bronze.customers"
)
```

The Bronze layer is responsible for initial ingestion while keeping the source data close to its original structure.

---

## 🥈 Silver Layer

The Silver layer reads streaming data from the Bronze layer and performs data cleansing and standardization.

The Silver layer uses DLT tables such as:

```text
dlt_project.silver.customers
dlt_project.silver.employees
dlt_project.silver.products
dlt_project.silver.categories
dlt_project.silver.orders
dlt_project.silver.order_details
dlt_project.silver.shippers
dlt_project.silver.suppliers
dlt_project.silver.shipments
```

### Transformations

The Silver layer performs:

* Column renaming
* Data standardization
* Adding `updated_time`
* Data quality validation
* Null checks on key columns
* Streaming reads from Bronze tables

For example, the customer table validates that:

```text
customer_id IS NOT NULL
```

using a DLT expectation.

```python
@dp.expect_all_or_drop({
    "rule_1": "customer_id is not null"
})
```

Similar data quality rules are applied to other important business keys.

---

## 🥇 Gold Layer

The Gold layer contains analytical dimensions and a fact table.

The project creates:

### Dimension Tables

```text
dim_customers
dim_employees
dim_products
dim_shipments
```

### Fact Table

```text
fact_orders
```

The Gold layer uses **SCD Type 2** for maintaining historical versions of dimension records.

---

# 🔄 SCD Type 2

SCD Type 2 is implemented using the DLT Auto CDC functionality.

The pipeline creates CDC views from Silver streaming tables and applies changes to Gold dimension tables.

Example:

```python
dp.create_auto_cdc_flow(
    target="dlt_project.gold.dim_customers",
    source="customers_cdc",
    keys=["customer_id"],
    sequence_by=col("updated_time"),
    stored_as_scd_type=2
)
```

### SCD Type 2 Process

When a new business record arrives:

```text
New Record
    │
    ▼
Insert into Gold
    │
    ▼
Current Record
```

When an existing record changes:

```text
Existing Current Record
          │
          ▼
      Expire Record
          │
          ▼
   Insert New Version
          │
          ▼
     Current Record
```

The Gold dimension tables maintain:

```text
__START_AT
__END_AT
```

to represent the historical validity of each record.

---

# 👤 Customer Dimension

The customer dimension uses:

```text
customer_id
```

as the business key.

The table contains customer attributes such as:

* Company Name
* Contact Name
* Contact Title
* Address
* City
* Region
* Postal Code
* Country
* Phone
* Fax

A surrogate key is generated using:

```text
customer_sk
```

---

# 👨‍💼 Employee Dimension

The employee dimension uses:

```text
employee_id
```

as the business key.

It contains:

* Employee Name
* Title
* City
* Country
* Reports To
* Updated Time

SCD Type 2 is applied using:

```text
employee_id
```

as the key.

---

# 📦 Product Dimension

The product dimension combines information from:

```text
Products
    +
Categories
    +
Suppliers
```

The Gold layer joins these Silver datasets before creating the product dimension.

The resulting dimension contains:

* Product
* Category
* Pricing
* Inventory
* Supplier
* Supplier Contact Information

The product dimension uses:

```text
product_id
```

as the business key and maintains historical versions using SCD Type 2.

---

# 🚚 Shipment Dimension

The shipment dimension combines:

```text
Shipments
    +
Shippers
```

The join is performed using the shipping method / shipper relationship.

The resulting Gold dimension contains:

* Order ID
* Shipped Date
* Ship Name
* Shipper Name
* Shipping Address
* Shipping City
* Shipping Region
* Shipping Postal Code
* Shipping Country
* Shipper Phone

SCD Type 2 is applied using:

```text
order_id
```

as the key.

---

# 📊 Fact Orders

The Gold layer also creates:

```text
fact_orders
```

as a materialized view.

The fact table combines:

```text
Orders
    +
Order Details
```

using:

```text
order_id
```

as the join key.

The project calculates:

### Line Amount

```text
line_amount =
unit_price × quantity × (1 - discount)
```

### Total Order Amount

The total order amount is calculated using a window partitioned by:

```text
order_id
```

### Freight Allocation

Freight is allocated to order-detail records based on the contribution of each line item to the total order amount.

```text
freight_charges =
order_freight
×
line_amount
/
total_order_amount
```

This produces a more detailed analytical representation of order-level freight.

---

# 📐 Data Model

The Gold layer can be represented as:

```text
                  ┌─────────────────┐
                  │  dim_customers  │
                  │ customer_id     │
                  └────────┬────────┘
                           │
                           │
┌─────────────────┐        ▼              ┌─────────────────┐
│ dim_employees   │────► fact_orders ◄────│ dim_products    │
│ employee_id     │        ▲              │ product_id      │
└─────────────────┘        │              └─────────────────┘
                           │
                           │
                  ┌────────┴────────┐
                  │ dim_shipments   │
                  │ order_id        │
                  └─────────────────┘
```

---

# 📂 Project Structure

```text
databricks-dlt-project/
│
├── README.md
│
├── notebooks/
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
│
├── architecture/
│   └── architecture.png
│
├── data_model/
│   └── data_model.png
│
└── .gitignore
```

---

# 📓 Pipeline Components

## `bronze.py`

Responsible for:

* Cloud file ingestion
* Auto Loader
* Streaming ingestion
* Creating Bronze DLT tables
* Reading source datasets from cloud storage

---

## `silver.py`

Responsible for:

* Reading Bronze streaming tables
* Data cleansing
* Column standardization
* Data quality expectations
* Adding update timestamps
* Creating Silver DLT tables

---

## `gold.py`

Responsible for:

* Creating Gold streaming tables
* Creating CDC views
* Applying SCD Type 2
* Joining related Silver datasets
* Creating dimension tables
* Creating the `fact_orders` materialized view
* Calculating order amounts
* Allocating freight charges

---

# 🔄 End-to-End Data Flow

```text
AWS S3
  │
  │ CSV Files
  ▼
Auto Loader
  │
  ▼
Bronze DLT Tables
  │
  │ Streaming Transformations
  ▼
Silver DLT Tables
  │
  ├───────────────┐
  │               │
  ▼               ▼
CDC Views      Fact Processing
  │               │
  ▼               ▼
SCD Type 2      fact_orders
  │
  ▼
Gold Dimensions
```

---

# 🎯 Key Data Engineering Concepts Demonstrated

* Medallion Architecture
* Delta Live Tables
* Spark Structured Streaming
* Databricks Auto Loader
* Delta Lake
* Data Quality Expectations
* Change Data Capture
* SCD Type 2
* Streaming Tables
* Materialized Views
* Dimension Modeling
* Fact Table Design
* Surrogate Keys
* Stream-to-stream joins
* Data transformation using PySpark
* Window Functions
* Freight Allocation
* Cloud-based data ingestion

---

# 🚀 Learning Outcomes

This project demonstrates how to build a modern data pipeline in Databricks from raw cloud data to analytics-ready Gold tables.

The project covers:

1. Ingesting cloud files using Auto Loader.
2. Building Bronze, Silver, and Gold layers.
3. Applying data quality rules.
4. Transforming streaming data using PySpark.
5. Building CDC flows.
6. Implementing SCD Type 2.
7. Creating analytical dimensions.
8. Creating a fact table.
9. Performing order-level calculations.
10. Allocating freight charges to order-detail records.
11. Building a pipeline suitable for downstream analytics.

---

# 👨‍💻 Author

**Manthan Bari**

Data Engineering Portfolio Project

### Technologies

Python | PySpark | Apache Spark | Databricks | Delta Lake | Delta Live Tables | Structured Streaming | AWS S3

````

