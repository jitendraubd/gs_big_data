# gs_big_data
Pyspark code for transformation gs big data problem written in databricks
Step1:First upload gz files to S3 public bucket
Step2: Read these data to databricks using pyspark code in notebook
Step3: After applying normalization (not null, unique and foreign constraints check) do broadcast join to fact table to get staged fact data
Step4: Create a refined table called mview_weekly_sales which totals sales_units, 
sales_dollars, and discount_dollars by pos_site_id, sku_id, fsclwk_id, 
price_substate_id and type.  
Step5: Write to SQL Warehouse after checking existing data
Step6. Link databrikcs to Github and commit code
