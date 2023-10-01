#!/bin/bash
spark3-submit --class com.databricks.spark.sql.perf.tpcds.GenTPCDSData \
	      --deploy-mode cluster \
	      --master yarn \
  	      --jars "./spark-sql-perf.jar" \
              --archives tpcds.tar.gz#tpcds \
	      --executor-cores 3 \
              --conf "spark.hadoop.parquet.memory.pool.ratio=0.1" \
              --conf "spark.executor.memoryOverhead=10g" \
	      --conf "spark.executor.memory=16g" \
	      --conf "spark.executor.cores=3" \
              --conf "spark.memory.fraction=0.05" \
	      --conf "spark.executor.instances=106" \
	      --conf "spark.driver.memory=16g" \
	      --conf "spark.driver.memoryOverhead=8g" \
	      --conf "spark.driver.cores=3" \
	      --conf "spark.blacklist.enabled=false" \
	      --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
	      --conf "spark.kryoserializer.buffer.max=2000" \
              --conf "spark.sql.shuffle.partitions=2000" \
	      --conf "spark.rdd.compress=true" \
              --conf "spark.sql.files.maxRecordsPerFile=2000000" \
              "./spark-sql-perf.jar" \
			 -d ./tpcds \
			 -m yarn \
			 -s 10000 \
			 -l "hdfs:///tests/tpcds/tpcds10tb" \
			 -f ORC \
			 -o true \
                         -p false \
                         -c false \
                         -n 3000

