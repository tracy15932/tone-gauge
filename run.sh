spark-submit \
--master spark://ip-10-0-0-11:7077 \
--deploy-mode client \
--executor-memory 5G \
--driver-memory 5G \
--num-executors 4 \
--executor-cores 3 \
--conf spark.executor.memoryOverhead=1124 \
--jars /usr/local/spark/jars/hadoop-aws-2.7.3.jar,/usr/local/spark/jars/aws-java-sdk-1.7.4.jar,/usr/local/spark/jars/mysql-connector-java-8.0.19.jar \
~/spark-job/ingestion.py

#run all the files in ingestion and data-processing folders on Spark Cluster