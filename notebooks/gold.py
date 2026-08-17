# =================================================
# IMPORTS
# =================================================
from pyspark import pipelines as dp
from pyspark.sql.functions import col

# ============================================================
# 1. CDC SOURCE
# ============================================================

@dp.view(
    name="customers_cdc"
)
def customers_cdc():

    return (
        spark.readStream
        .table("dlt_project.silver.customers")
    )


# ============================================================
# 2. CREATE GOLD TARGET
# ============================================================

dp.create_streaming_table(
    name="dlt_project.gold.dim_customers",
    schema="""
        customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
        customer_id STRING,
        company_name STRING,
        contact_name STRING,
        contact_title STRING,
        cust_address STRING,
        city STRING,
        region STRING,
        postal_code STRING,
        country STRING,
        phone STRING,
        fax STRING,
        updated_time TIMESTAMP,
        __START_AT TIMESTAMP,
        __END_AT TIMESTAMP
    """
)

# ============================================================
# 3. APPLY SCD TYPE 2
# ============================================================

dp.create_auto_cdc_flow(
    target="dlt_project.gold.dim_customers",

    source="customers_cdc",

    keys=["customer_id"],

    sequence_by=col("updated_time"),

    stored_as_scd_type=2,

    except_column_list=["_rescued_data"]
)

# employees

from pyspark import pipelines as dp
from pyspark.sql.functions import col


# ============================================================
# 1. CDC SOURCE
# ============================================================

@dp.view(
    name="employees_cdc"
)
def employees_cdc():

    return (
        spark.readStream
        .table("dlt_project.silver.employees")
    )


# ============================================================
# 2. CREATE GOLD TARGET
# ============================================================

dp.create_streaming_table(
    name="dlt_project.gold.dim_employees",
    schema="""
        employee_id			            INT,
        employee_name			        STRING,
        title			                STRING,
        city			                STRING,
        country			                STRING,
        reportsTo		                INT,
        updated_time                    TIMESTAMP,
        __START_AT TIMESTAMP,
        __END_AT TIMESTAMP
    """
)

# ============================================================
# 3. APPLY SCD TYPE 2
# ============================================================

dp.create_auto_cdc_flow(
    target="dlt_project.gold.dim_employees",

    source="employees_cdc",

    keys=["employee_id"],

    sequence_by=col("updated_time"),

    stored_as_scd_type=2,

    except_column_list=["_rescued_data"]
)


# products
@dp.view(name="products_cdc")
def products_cdc():

    categories = spark.readStream.table("dlt_project.silver.categories")
    products = spark.readStream.table("dlt_project.silver.products")
    suppliers = spark.readStream.table("dlt_project.silver.suppliers")

    df = products.alias("p").join(
    categories.alias("c"),
    col("p.category_id") == col("c.category_id")
    ).join(suppliers.alias("s"),
    col("p.supplier_id") == col("s.supplier_id")
    ).select(
    col("p.product_id"),
    col("p.product_name"),
    col("c.category_name"),
    col("p.quantity_per_unit"),
    col("p.unit_price"),
    col("p.units_in_stock"),
    col("p.units_on_order"),
    col("p.reorder_level"),
    col("p.discontinued"),
    col("s.company_name").alias("supplier_name"),
    col("s.contact_name").alias("supplier_contact"),
    col("s.contact_title").alias("supplier_contact_title"),
    col("s.address").alias("supplier_address"),
    col("s.city").alias("supplier_city"),
    col("s.region").alias("supplier_region"),
    col("s.postal_code").alias("supplier_postal_code"),
    col("s.country").alias("supplier_country"),
    col("s.phone").alias("supplier_phone"),
    col("s.fax").alias("supplier_fax"),
    col("p.updated_time")
)
    return (df)


dp.create_streaming_table(
    name="dlt_project.gold.dim_products",
    schema = """
                product_sk			      BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                product_id			      INT,
                product_name		            STRING,
                category_name			      STRING,
                quantity_per_unit		      STRING,
                unit_price			      DOUBLE,
                units_in_stock		      INT,
                units_on_order		      INT,
                reorder_level		            INT,
                discontinued		            BOOLEAN,
                supplier_name			      STRING,
                supplier_contact		      STRING,
                supplier_contact_title          STRING,
                supplier_address		      STRING,
                supplier_city		            STRING,
                supplier_region		      STRING,
                supplier_postal_code            STRING,
                supplier_country		      STRING,
                supplier_phone		      STRING,
                supplier_fax		            STRING,
                updated_time                    TIMESTAMP,
                __START_AT TIMESTAMP,
                __END_AT TIMESTAMP
                """)


dp.create_auto_cdc_flow(
    target="dlt_project.gold.dim_products",
    source="products_cdc",
    keys=["product_id"],
    sequence_by=col("updated_time"),
    stored_as_scd_type=2
)

#shipments
@dp.view(name="shipments_cdc")
def shipments_cdc():

    shippers = spark.readStream.table("dlt_project.silver.shippers")
    shipments = spark.readStream.table("dlt_project.silver.shipments")

    df = shipments.alias("sm").join(
        shippers.alias("sp"),col("sm.ship_via") == col("sp.shipper_id")
        ).select(col("sm.order_id"),
        col("sm.shipped_date"),
        col("sm.ship_name"),
        col("sp.company_name").alias("shipper_name"),
        col("sm.ship_address"),
        col("sm.ship_city"),
        col("sm.ship_region"),
        col("sm.ship_postal_code"),
        col("sm.ship_country"),
        col("sp.phone").alias("shipper_phone"),
        col("sm.updated_time")
    )
    return (df)


dp.create_streaming_table(
    name="dlt_project.gold.dim_shipments",
    schema = """
                shipment_sk		      BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                order_id			      INT,
                shipped_date		      TIMESTAMP,
                ship_name		            STRING,
                shipper_name		      STRING,
                ship_address		      STRING,
                ship_city		            STRING,
                ship_region		      STRING,
                ship_postal_code            STRING,
                ship_country		      STRING,
                shipper_phone		      STRING,
                updated_time                    TIMESTAMP,
                __START_AT TIMESTAMP,
                __END_AT TIMESTAMP
                """)


dp.create_auto_cdc_flow(
    target="dlt_project.gold.dim_shipments",
    source="shipments_cdc",
    keys=["order_id"],
    sequence_by=col("updated_time"),
    stored_as_scd_type=2
)

#fact_table
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window


@dp.materialized_view(
    name="dlt_project.gold.fact_orders"
)
def fact_orders():

    # Read Silver as BATCH
    orders = spark.read.table(
        "dlt_project.silver.orders"
    )

    order_details = spark.read.table(
        "dlt_project.silver.order_details"
    )

    # Join Orders + Order Details
    df = (
        orders.alias("o")
        .join(
            order_details.alias("od"),
            col("o.order_id") == col("od.order_id"),
            "inner"
        )
    )

    # Calculate line amount
    df = df.withColumn(
        "line_amount",
        col("od.unit_price")
        * col("od.quantity")
        * (1 - col("od.discount"))
    )

    # Window by order
    w = Window.partitionBy("o.order_id")

    # Calculate total order amount
    df = df.withColumn(
        "total_order_amount",
        sum("line_amount").over(w)
    )

    # Allocate freight
    df = df.withColumn(
        "freight_charges",
        round(
            col("o.freight")
            * col("line_amount")
            / col("total_order_amount"),
            2
        )
    )

    # Final fact table
    return df.select(
        col("od.order_id")
            .cast("int")
            .alias("order_id"),

        col("o.customer_id")
            .cast("string")
            .alias("customer_id"),

        col("od.product_id")
            .cast("int")
            .alias("product_id"),

        col("od.unit_price")
            .cast("double")
            .alias("unit_price"),

        col("od.quantity")
            .cast("int")
            .alias("quantity"),

        col("od.discount")
            .cast("double")
            .alias("discount"),

        col("freight_charges")
            .cast("double")
            .alias("freight_charges"),

        col("o.employee_id")
            .cast("int")
            .alias("employee_id"),

        col("o.ship_via")
            .cast("int")
            .alias("shipper_id"),

        col("o.order_date")
            .cast("timestamp")
            .alias("order_date"),

        col("o.shipped_date")
            .cast("timestamp")
            .alias("shipped_date")
    )
