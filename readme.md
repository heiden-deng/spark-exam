
任务提交命令
spark-submit --class com.heiden.spark.PeopleInfoCalculator --master spark://localhost:7077 --executor-memory 500m --total-executor-cores 1 --driver-memory 1024m --driver-java-options "-Dspark.testing.memory=1073741824" /jars/target/api-test-1.0-SNAPSHOT.jar spark://localhost:7077 /spark/person.txt
