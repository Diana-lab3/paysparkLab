##sales Query##

####
spark = (SparkSession.builder.appName("SalesDataPrep").getOrCreate)

File/Upload Data

%md Read CSV Files into DataFrame
schema = StructTipe([
   StructField(Order ID, StringType(),true),
   StructField(Product, StringType(),true),
   StructField(Quantity Ordered, StringType(),true),
   StructField(Price Each, StringType(),true),
   StructField(Order Date, StringType(),true),
   StructField(Prchase Adress, StringType(),true)
   ])
########
sale_data_fpath = "bdfs:/FileStore/salesdata/raw"
sales_raw_df = (spark.read.format("csv")
                 .option("header", True)
                 .schema(schema)
                 .load(sales_data_fpath)
   )

sales_raw_df.show(10)

sales_raw_df.printShema()

use sales_db

########
select * 
from sales raw
limit 10


########
WITH tem_sales_raw as(
  select *
from sales_db.sales_raw
where OrderID != "Order ID"
  and OrderID is not null
)
select 
* from tem_sales_raw
limit 10

########
WITH temp_sales_raw AS (
  select * 
from sales_db.sales_raw
where OrderID != "Order ID"
  and Order ID is not null
)
select 
* from temp_sales_raw
where Order ID is null
   or Order ID = "Order ID"
limit 10

########
select split(PurchaseAdress, ',') as City
from sales_raw
where OrderID != "Order ID"
  and OrderID is not null
limit 10