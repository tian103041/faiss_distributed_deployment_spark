package test

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object test {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("faiss_kmeans").getOrCreate()
    val   sc  = spark.sparkContext

    import spark.implicits._
    val list_df_groupby = Seq(("a",10),("a",15),("b",20),("c",20),("b",87))
    val test_df_groupby = sc.parallelize(list_df_groupby).toDF("name","num")

    test_df_groupby.write.parquet("hdfs://region4/region4/29297/app/develop/faiss_distribute/test")

    val groupby = test_df_groupby.groupBy("name")

    val groupby_join  = groupby.count().join(test_df_groupby,"name")

    for(name <- List("a","b","c")){
      groupby_join.filter($"name"===name).write.csv(s"/user/11085098/faiss/test_csv_$name")
    }

    val my_test2 = spark.read.csv("/user/11085098/faiss/test_csv_a")


    val slave_array = Array("10.102.23.107","10.102.23.108","10.102.23.109")
    val numClusters = 3
    val array_slave = ArrayBuffer[(Int,String)]()
    for (i <- 0 until numClusters-1){
      array_slave.append((i,slave_array(i)))
    }

    array_slave.toArray
  }



}
