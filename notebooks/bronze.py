# =================================================
# IMPORT
# =================================================
from pyspark import pipelines as dp
from pyspark.sql.functions import *

# ============================================================
# CUSTOMERS
# ============================================================
@dp.table(
    name="dlt_project.bronze.customers"
)
def customers():

      df=spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","csv")\
            .option("haeder","true")\
            .option("cloudFiles.inferColumnTypes","true")\
            .option("quote",'"')\
            .option("escape",'"')\
            .option("multiLine","true")\
            .load("s3://manthan27/N_Wind/customers/")

      return(df)

# ============================================================
# EMPLOYEES
# ============================================================
@dp.table(
    name="dlt_project.bronze.employees"
)
def customers():

      df=spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","csv")\
            .option("haeder","true")\
            .option("cloudFiles.inferColumnTypes","true")\
            .option("quote",'"')\
            .option("escape",'"')\
            .option("multiLine","true")\
            .load("s3://manthan27/N_Wind/employees/")

      return(df)

# ============================================================
# PRODUCTS
# ============================================================
@dp.table(
    name="dlt_project.bronze.products"
)
def customers():

      df=spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","csv")\
            .option("haeder","true")\
            .option("cloudFiles.inferColumnTypes","true")\
            .option("quote",'"')\
            .option("escape",'"')\
            .option("multiLine","true")\
            .load("s3://manthan27/N_Wind/products/")

      return(df)

# ============================================================
# CATEGORIES
# ============================================================
@dp.table(
    name="dlt_project.bronze.categories"
)
def customers():

      df=spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","csv")\
            .option("haeder","true")\
            .option("cloudFiles.inferColumnTypes","true")\
            .option("quote",'"')\
            .option("escape",'"')\
            .option("multiLine","true")\
            .load("s3://manthan27/N_Wind/categories/")

      return(df)

# ============================================================
# ORDERS
# ============================================================
@dp.table(
    name="dlt_project.bronze.orders"
)
def customers():

      df=spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","csv")\
            .option("haeder","true")\
            .option("cloudFiles.inferColumnTypes","true")\
            .option("quote",'"')\
            .option("escape",'"')\
            .option("multiLine","true")\
            .load("s3://manthan27/N_Wind/orders/")

      return(df)

# ============================================================
# ORDER DETAILS
# ============================================================
@dp.table(
    name="dlt_project.bronze.order_details"
)
def customers():

      df=spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","csv")\
            .option("haeder","true")\
            .option("cloudFiles.inferColumnTypes","true")\
            .option("quote",'"')\
            .option("escape",'"')\
            .option("multiLine","true")\
            .load("s3://manthan27/N_Wind/order_details/")

      return (
        df
        .withColumnRenamed("Product Name", "product_name")
      )

# ============================================================
# SHIPPERS
# ============================================================
@dp.table(
    name="dlt_project.bronze.shippers"
)
def customers():

      df=spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","csv")\
            .option("haeder","true")\
            .option("cloudFiles.inferColumnTypes","true")\
            .option("quote",'"')\
            .option("escape",'"')\
            .option("multiLine","true")\
            .load("s3://manthan27/N_Wind/shippers/")

      return(df)

# ============================================================
# SUPPLIERS
# ============================================================
@dp.table(
    name="dlt_project.bronze.suppliers"
)
def customers():

      df=spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","csv")\
            .option("haeder","true")\
            .option("cloudFiles.inferColumnTypes","true")\
            .option("quote",'"')\
            .option("escape",'"')\
            .option("multiLine","true")\
            .load("s3://manthan27/N_Wind/suppliers/")

      return(df)

# ============================================================
# SHIPMENTS
# ============================================================
@dp.table(
    name="dlt_project.bronze.shipments"
)
def customers():

      df=spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","csv")\
            .option("haeder","true")\
            .option("cloudFiles.inferColumnTypes","true")\
            .option("quote",'"')\
            .option("escape",'"')\
            .option("multiLine","true")\
            .load("s3://manthan27/N_Wind/shipments/")

      return(df)

