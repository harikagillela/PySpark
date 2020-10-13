from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark = SparkSession.builder.master('local').appName('dailyTopProducts').getOrCreate()
spark.conf.set('spark.sql.shuffle.partitions','5')
orderitems = spark.read.format('json').load('/Users/harikagillela/Hadoop/data/retail_db_json/order_items')
orders = spark.read.csv("/Users/harikagillela/Hadoop/data/retail_db/orders", 
	schema = "order_id int, order_date string, order_customer_id int, order_status string")

spec_dailyprod = Window.partitionBy('order_date').orderBy(col('order_revenue').desc())

dailyTopProducts = orders.join(orderitems, orders.order_id == orderitems.order_item_order_id, 'inner').\
	groupby('order_date','order_item_product_id').agg(round(sum('order_item_subtotal'),2).alias('order_revenue')).\
	withColumn('prod_rank', rank().over(spec_dailyprod)).\
	filter(col('prod_rank')<=5).\
	sort('order_date',col('order_revenue').desc())

dailyTopProducts.write.csv('/Users/harikagillela/Hadoop/pyspark_files/Results_csv')
dailyTopProducts.write.orc('/Users/harikagillela/Hadoop/pyspark_files/Results_orc')
dailyTopProducts.write.parquet('/Users/harikagillela/Hadoop/pyspark_files/Results_parquet')
