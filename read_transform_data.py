from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Initialize Spark Session
spark = SparkSession.builder.appName("ReadPublicS3").getOrCreate()

def check_not_null(df,col):
    if df.filter(df[col].isNull()).count() > 0:
        raise ValueError(f"Primary key column {col} contains nulls")

def check_unique(df,col):
    if df.groupBy(col).count().filter("count > 1").count() > 0:
        raise ValueError("Primary key constraint violation")
    

def check_data_type(fact_df,dim_df,pk_col,fk_col):
    if fact_df.schema[fk_col].dataType != dim_df.schema[pk_col].dataType:
        raise ValueError(f"Primary key and Foreign key data type is not same for {fk_col}")

def check_fk_constraints(fact_df,dim_df,pk_col,fk_col):
    invalid_key = fact_df.join(dim_df, fact_df[fk_col] == dim_df[pk_col],"left_anti")
    if invalid_key.count() > 0:
        raise ValueError(f"Foreign key constraint violation in {fk_col}")

# S3 Public URL
s3_possite_path = "s3a://my-gs-data/hier.possite.dlm.gz"
s3_pricestate_path = "s3a://my-gs-data/hier.pricestate.dlm.gz"
s3_prod_path = "s3a://my-gs-data/hier.prod.dlm.gz"
s3_clnd_path = "s3a://my-gs-data/hier.clnd.dlm.gz"
s3_transactions_path = "s3a://my-gs-data/fact.transactions.dlm.gz"

# Read CSV File
dim_possite_df = spark.read.format("csv")\
            .option("header","true")\
            .option("inferSchema","true")\
            .option("delimiter","|")\
            .load(s3_possite_path)
dim_possite_df.show()
dim_pricestate_df = spark.read.format("csv")\
            .option("header","true")\
            .option("inferSchema","true")\
            .option("delimiter","|")\
            .load(s3_pricestate_path)

dim_prod_df = spark.read.format("csv")\
            .option("header","true")\
            .option("inferSchema","true")\
            .option("delimiter","|")\
            .load(s3_prod_path)

dim_clnd_df = spark.read.format("csv")\
            .option("header","true")\
            .option("inferSchema","true")\
            .option("delimiter","|")\
            .load(s3_clnd_path)

# Read Fact data with filtered with sale type otherwise getting duplicate data
fact_transactions_df = spark.read.format("csv")\
            .option("header","true")\
            .option("inferSchema","true")\
            .option("delimiter","|")\
            .load(s3_transactions_path).filter(col("type") == "sale").distinct()

# Check primary key is not null
check_not_null(dim_possite_df,"site_id")
check_not_null(dim_pricestate_df,"substate_id")
check_not_null(dim_prod_df,"sku_id")
check_not_null(dim_clnd_df,"fscldt_id")
check_not_null(fact_transactions_df,"order_id")

# Check primary key is unique
check_unique(dim_possite_df,"site_id")
check_unique(dim_pricestate_df,"substate_id")
check_unique(dim_prod_df,"sku_id")
check_unique(dim_clnd_df,"fscldt_id")
check_unique(fact_transactions_df,"order_id")

# Check for data type of foreign key in fact table and primary key in dimentional table
check_data_type(fact_transactions_df,dim_possite_df,"site_id","pos_site_id")
check_data_type(fact_transactions_df,dim_pricestate_df,"substate_id","price_substate_id")
check_data_type(fact_transactions_df,dim_prod_df,"sku_id","sku_id")
check_data_type(fact_transactions_df,dim_clnd_df,"fscldt_id","fscldt_id")

# Check for foreign key constraints in fact table
check_fk_constraints(fact_transactions_df,dim_possite_df,"site_id","pos_site_id")
check_fk_constraints(fact_transactions_df,dim_pricestate_df,"substate_id","price_substate_id")
check_fk_constraints(fact_transactions_df,dim_prod_df,"sku_id","sku_id")
check_fk_constraints(fact_transactions_df,dim_clnd_df,"fscldt_id","fscldt_id")

# Create a staging schema where the hierarchy table has been normalized into a table for each level 
# and the staged fact table has foreign key relationships with those tables

normalized_site_df = dim_possite_df.select(col("site_id"), col("site_label"), col("subchnl_id")).distinct()
normalize_subchannel_df = dim_possite_df.select(col("subchnl_id"), col("subchnl_label"), col("chnl_id")).distinct()
normalize_channel_df = dim_possite_df.select(col("chnl_id"), col("chnl_label")).distinct()
normalized_site_df.show()
normalize_substate_df = dim_pricestate_df.select(col("substate_id"), col("substate_label"), col("state_id")).distinct()
normalize_state_df = dim_pricestate_df.select(col("state_id"), col("state_label")).distinct()

normalized_prod_df = dim_prod_df.select(col("sku_id"), col("stylclr_id"), col("styl_id"), col("subcat_id"), col("dept_id"), col("issvc"), col("isasmbly"), col("isnfs")).distinct()
normalized_sku_df = dim_prod_df.select(col("sku_id"), col("sku_label")).distinct()
normalize_styleclr_df = dim_prod_df.select(col("stylclr_id"), col("stylclr_label")).distinct()
normalize_style_df = dim_prod_df.select(col("styl_id"), col("styl_label")).distinct()
normalize_subcategory_df = dim_prod_df.select(col("subcat_id"), col("subcat_label"), col("cat_id")).distinct()
normalize_category_df = dim_prod_df.select(col("cat_id"), col("cat_label")).distinct()
normalize_department_df = dim_prod_df.select(col("dept_id"), col("dept_label")).distinct()

normalized_clnd_df = dim_clnd_df.select(col("fscldt_id"), col("fscldt_label"), col("fsclwk_id")).distinct()
normalized_clnd_trans_df = dim_clnd_df.select(col("fscldt_id"), col("fsclwk_id"), col("fsclmth_id"), col("fsclqrtr_id"), col("fsclyr_id"), col("ssn_id"), col("ly_fscldt_id"), col("lly_fscldt_id"), col("fscldow"), col("fscldom"), col("fscldoq"), col("fscldoy"), col("fsclwoy"), col("fsclmoy"), col("fsclqoy"), col("date")).distinct()
normalize_fsclwk_df = dim_clnd_df.select(col("fsclwk_id"), col("fsclwk_label")).distinct()
normalize_fsclmth_df = dim_clnd_df.select(col("fsclmth_id"), col("fsclmth_label")).distinct()
normalize_fsclqrtr_df = dim_clnd_df.select(col("fsclqrtr_id"), col("fsclqrtr_label")).distinct()
normalize_fsclyr_df = dim_clnd_df.select(col("fsclyr_id"), col("fsclyr_label")).distinct()
normalize_ssn_df = dim_clnd_df.select(col("ssn_id"), col("ssn_label")).distinct()

#staged fact table has foreign key  relationships with those tables
# Broadcast all dimension tables during joins

staged_fact_df = fact_transactions_df.alias("fact") \
    .join(broadcast(normalized_site_df.alias("site")), 
          col("fact.pos_site_id") == col("site.site_id"), "inner") \
    .join(broadcast(normalized_sku_df.alias("sku")), 
          col("fact.sku_id") == col("sku.sku_id"), "inner") \
    .join(broadcast(normalized_clnd_df.alias("clnd")), 
          col("fact.fscldt_id") == col(clnd.fscldt_id"), "inner") \
    .join(broadcast(normalize_substate_df.alias("substate")), 
          col("fact.price_substate_id") == col("substate.substate_id"), "inner") \
    .select(
        col("fact.order_id").alias("order_id"),
        col("fact.line_id").alias("line_id"),
        col("fact.pos_site_id").alias("pos_site_id"),
        col("fact.sku_id").alias("sku_id"),
        col("clnd.fsclwk_id").alias("fsclwk_id"),
        col("fact.price_substate_id").alias("price_substate_id"),
        col("fact.type").alias("type"),
        col("fact.sales_units").alias("sales_units"),
        col("fact.sales_dollars").alias("sales_dollars"),
        col("discount_dollars").alias("discount_dollars")
    )

# Aggregation to create refined table 'mview_weekly_sales'
mview_weekly_sales_df = staged_fact_df.groupBy("pos_site_id", "sku_id", "fsclwk_id", "price_substate_id", "type").agg(
    sum("sales_units").alias("total_sales_units"),
    sum("sales_dollars").alias("total_sales_dollars"),
    sum("discount_dollars").alias("total_discount_dollars")
)

# Define SQL warehouse connection properties
jdbc_url = "jdbc:databricks://<server-hostname>:443/default;transportMode=http;ssl=1;httpPath=<http-path>"
connection_properties = {
    "user": "<your-username>",
    "password": "<your-password>",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Load existing data for incremental update
existing_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "mview_weekly_sales") \
    .option("user", connection_properties["user"]) \
    .option("password", connection_properties["password"]) \
    .option("driver", connection_properties["driver"]) \
    .load()

# Perform incremental update by merging new and existing data
incremental_df = existing_df.union(mview_weekly_sales_df) \
    .groupBy("pos_site_id", "sku_id", "fsclwk_id", "price_substate_id", "type").agg(
        sum("total_sales_units").alias("total_sales_units"),
        sum("total_sales_dollars").alias("total_sales_dollars"),
        sum("total_discount_dollars").alias("total_discount_dollars")
    )

# Write transformed data to SQL warehouse
df_incremental.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "mview_weekly_sales") \
    .option("user", connection_properties["user"]) \
    .option("password", connection_properties["password"]) \
    .option("driver", connection_properties["driver"]) \
    .mode("overwrite") \
    .save()

print("Data successfully written to SQL warehouse")

