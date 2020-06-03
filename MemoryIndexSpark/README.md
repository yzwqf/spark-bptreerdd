1. 将本文件夹的内容放至$SPARK_HOME/spark/examples/src/main/scala/org/apache/spark/examples文件夹下
2. 向hdfs中put需要的数据，并记录文件的URI，下用fsuri代替
3. 编译spark
4. $SPARK_HOME/spark/bin/run-example BptLogQuery fsuri