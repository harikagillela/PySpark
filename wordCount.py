pyspark \
--master yarn \
--conf spark.ui.port=12456 \
--num-executors 10 \
--executor-memory 3G \
--executor-cores 2 \
--packages com.databricks:spark-avro_2.11:4.0.0

data=spark.read.text('/public/randomtextwriter')

wc=data.select(explode(split(data.value,' ')).alias('words')).\
... groupBy(col('words')).agg(count('words').alias('count'))

wc.coalesce(8).\
... write.format('com.databricks.spark.avro').\
... save('/user/akshith96/hg/results/wc_avro')

