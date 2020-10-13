orderitems = spark.read.format('json').load('/Users/harikagillela/Hadoop/data/retail_db_json/order_items')

orders =spark.read.format("csv").option('sep',',').\
schema("order_id int, order_date string, order_customer_id int, order_status string").\
load("/public/retail_db/orders")

spec_orderdate = Window.partitionBy('order_date')
spec_orderitem = Window.partitionBy('order_item_order_id')

ordersAvgRevenue = orders.join(orderitems, orders.order_id == orderitems.order_item_order_id, "inner").\
select('order_date','order_item_id','order_item_subtotal').\
withColumn('order_revenue',round(sum('order_item_subtotal').over(spec_orderitem),2)).\
withColumn('avg_revenue',round(avg(col('order_revenue')).over(spec_orderdate),2)).\
filter(col('order_revenue')>col('avg_revenue')).\
sort('order_item_order_id')


orders75 = orders.join(orderitems, orders.order_id == orderitems.order_item_order_id, "inner").\
select('order_date','order_item_order_id','order_item_subtotal').\
withColumn('order_revenue',round(sum('order_item_subtotal').over(spec_orderitem),2)).\
withColumn('HighestRevenue',max(col('order_revenue')).over(spec_orderdate)).\
filter(col('order_revenue') > 0.75*col('HighestRevenue')).\
sort('order_date','order_item_order_id')

