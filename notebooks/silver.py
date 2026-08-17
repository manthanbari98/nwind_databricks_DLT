# =================================================
# IMPORTS
# =================================================
from pyspark import pipelines as dp
from pyspark.sql.functions import *

# ============================================================
# CUSTOMERS
# ============================================================
@dp.table(
    name ="dlt_project.silver.customers"
    )

@dp.expect_all_or_drop({"rule_1":"customer_id is not null"})

def silver_customers():
    df = spark.readStream.table("dlt_project.bronze.customers")

    return(
            df.withColumnRenamed("CustomerID", "customer_id")
            .withColumnRenamed("CompanyName", "company_name")
            .withColumnRenamed("ContactName", "contact_name")
            .withColumnRenamed("ContactTitle", "contact_title")
            .withColumnRenamed("Address", "cust_address")
            .withColumnRenamed("City", "city")
            .withColumnRenamed("Region", "region")
            .withColumnRenamed("PostalCode", "postal_code")
            .withColumnRenamed("Country", "country")
            .withColumnRenamed("Phone", "phone")
            .withColumnRenamed("Fax", "fax")
            .withColumn("updated_time",current_timestamp())
    )

# ============================================================
# EMPLOYEES
# ============================================================
@dp.table(
    name ="dlt_project.silver.employees"
    )

@dp.expect_all_or_drop({"rule_1":"employee_id is not null"})

def silver_employees():
    df = spark.readStream.table("dlt_project.bronze.employees")

    return(
            (df
            .withColumnRenamed("employeeID", "employee_id")
            .withColumnRenamed("employeeName", "employee_name")
            .withColumnRenamed("title", "title")
            .withColumnRenamed("city", "city")
            .withColumnRenamed("country", "country")
            .withColumnRenamed("reportsTo", "reportsTo")
            .withColumn("updated_time", current_timestamp()))
    )

# ============================================================
# PRODUCTS
# ============================================================
@dp.table(
    name ="dlt_project.silver.products"
    )

@dp.expect_all_or_drop({"rule_1":"product_id is not null"})

def silver_products():
    df = spark.readStream.table("dlt_project.bronze.products")

    return(
            (df
            .withColumnRenamed("ProductID", "product_id")
            .withColumnRenamed("ProductName", "product_name")
            .withColumnRenamed("SupplierID","supplier_id")
            .withColumnRenamed("CategoryID", "category_id")
            .withColumnRenamed("QuantityPerUnit", "quantity_per_unit")
            .withColumnRenamed("UnitPrice", "unit_price")
            .withColumnRenamed("UnitsInStock", "units_in_stock")
            .withColumnRenamed("UnitsOnOrder", "units_on_order")
            .withColumnRenamed("ReorderLevel", "reorder_level")
            .withColumnRenamed("Discontinued", "discontinued")
            .withColumn("updated_time", current_timestamp()))
    )

# ============================================================
# CATEGORIES
# ============================================================
@dp.table(
    name ="dlt_project.silver.categories"
    )

@dp.expect_all_or_drop({"rule_1":"category_id is not null"})

def silver_categories():
    df = spark.readStream.table("dlt_project.bronze.categories")

    return(
            (df
            .withColumnRenamed("CategoryID", "category_id")
            .withColumnRenamed("CategoryName", "category_name")
            .withColumnRenamed("Description", "description")
            .withColumnRenamed("Picture", "picture")
            .withColumn("updated_time", current_timestamp()))
    )

# ============================================================
# ORDERS
# ============================================================
@dp.table(
    name ="dlt_project.silver.orders"
    )

@dp.expect_all_or_drop({"rule_1":"order_id is not null"})

def silver_orders():
    df = spark.readStream.table("dlt_project.bronze.orders")

    return(
            (df
            .withColumnRenamed("OrderID", "order_id")
            .withColumnRenamed("CustomerID", "customer_id")
            .withColumnRenamed("EmployeeID", "employee_id")
            .withColumnRenamed("OrderDate", "order_date")
            .withColumnRenamed("RequiredDate", "required_date")
            .withColumnRenamed("ShippedDate", "shipped_date")    
            .withColumnRenamed("ShipVia", "ship_via")
            .withColumnRenamed("Freight", "freight")
            .withColumnRenamed("ShipName", "ship_name")
            .withColumnRenamed("ShipAddress", "ship_address")
            .withColumnRenamed("ShipCity", "ship_city")
            .withColumnRenamed("ShipRegion", "ship_region")
            .withColumnRenamed("ShipPostalCode", "ship_postal_code")
            .withColumnRenamed("ShipCountry", "ship_country")
            .withColumn("updated_time", current_timestamp()))
    )

# ============================================================
# SHIPMENTS
# ============================================================
@dp.table(
    name ="dlt_project.silver.shipments"
    )

@dp.expect_all_or_drop({"rule_1":"order_id is not null"})

def silver_shipments():
    df = spark.readStream.table("dlt_project.bronze.shipments")

    return(
            (df
            .withColumnRenamed("OrderID", "order_id")
            .withColumnRenamed("CustomerID", "customer_id")
            .withColumnRenamed("EmployeeID", "employee_id")
            .withColumnRenamed("OrderDate", "order_date")
            .withColumnRenamed("RequiredDate", "required_date")
            .withColumnRenamed("ShippedDate", "shipped_date")    
            .withColumnRenamed("ShipVia", "ship_via")
            .withColumnRenamed("Freight", "freight")
            .withColumnRenamed("ShipName", "ship_name")
            .withColumnRenamed("ShipAddress", "ship_address")
            .withColumnRenamed("ShipCity", "ship_city")
            .withColumnRenamed("ShipRegion", "ship_region")
            .withColumnRenamed("ShipPostalCode", "ship_postal_code")
            .withColumnRenamed("ShipCountry", "ship_country")
            .withColumn("updated_time", current_timestamp()))
    )

# ============================================================
# ORDER DETAILS
# ============================================================
@dp.table(
    name ="dlt_project.silver.order_details"
    )

def silver_order_details():
    df = spark.readStream.table("dlt_project.bronze.order_details")

    return(
            df
            .withColumnRenamed("OrderID", "order_id")
            .withColumnRenamed("ProductID", "product_id")
            .withColumnRenamed("UnitPrice", "unit_price")
            .withColumnRenamed("Quantity", "quantity")
            .withColumnRenamed("Discount", "discount")
            .withColumn("updated_time", current_timestamp())
    )

# ============================================================
# SHIPPERS
# ============================================================
@dp.table(
    name ="dlt_project.silver.shippers"
    )

@dp.expect_all_or_drop({"rule_1":"shipper_id is not null"})

def silver_shippers():
    df = spark.readStream.table("dlt_project.bronze.shippers")

    return(
            df
      .withColumnRenamed("ShipperID", "shipper_id")
      .withColumnRenamed("CompanyName", "company_name")
      .withColumnRenamed("Phone", "phone")
      .withColumn("updated_time", current_timestamp())
    )

# ============================================================
# SUPPLIERS
# ============================================================
@dp.table(
    name ="dlt_project.silver.suppliers"
    )

@dp.expect_all_or_drop({"rule_1":"supplier_id is not null"})

def silver_suppliers():
    df = spark.readStream.table("dlt_project.bronze.suppliers")

    return(
            (df
      .withColumnRenamed("SupplierID", "supplier_id")
      .withColumnRenamed("CompanyName", "company_name")
      .withColumnRenamed("ContactName", "contact_name")
      .withColumnRenamed("ContactTitle", "contact_title")
      .withColumnRenamed("Address", "address")
      .withColumnRenamed("City", "city")
      .withColumnRenamed("Region", "region")
      .withColumnRenamed("PostalCode", "postal_code")
      .withColumnRenamed("Country", "country")
      .withColumnRenamed("Phone", "phone")
      .withColumnRenamed("Fax", "fax")
      .withColumnRenamed("HomePage", "home_page")
      .withColumn("updated_time", current_timestamp()))
    )
