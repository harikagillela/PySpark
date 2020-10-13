from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark = SparkSession.builder.appName('prodRevenue').master('local').getOrCreate()
spark.conf.set('spark.sql.shuffle.partitions','5')
orderitems = spark.read.format('json').load('/Users/harikagillela/Hadoop/data/retail_db_json/order_items')
products = spark.read.format('json').load('/Users/harikagillela/Hadoop/data/retail_db_json/products')

spec_prodcat = Window.partitionBy('product_category_id').orderBy(col('product_revenue').desc())
spec_prodrev = Window.partitionBy('product_category_id')

prodRevenueDiff = products.join(orderitems,products.product_id == orderitems.order_item_product_id, 'inner').\
	groupby('product_category_id','product_id','product_name').agg(round(sum('order_item_subtotal'),2).alias('product_revenue')).\
	withColumn('prod_rank',rank().over(spec_prodcat)).\
	withColumn('prod_dense_rank', dense_rank().over(spec_prodcat)).\
	withColumn('revenue_diff', round(max(col('product_revenue')).over(spec_prodrev) - col('product_revenue'),2)).\
	sort('product_category_id',col('product_revenue').desc())

prodRevenueDiff.write.csv('/Users/harikagillela/Hadoop/pyspark_files/ProdRevenue')
