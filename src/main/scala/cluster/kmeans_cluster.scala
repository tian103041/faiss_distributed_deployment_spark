package cluster

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.io.BufferedSource
import org.apache.spark.mllib.feature.PCA
import util.message_public
import scala.collection.mutable.ArrayBuffer

object kmeans_cluster {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("faiss_kmeans").getOrCreate()
    val   sc  = spark.sparkContext

    //总路径,质心及其向量的保存路径,adid及其映射的路径
    val    path_total   = "hdfs://region4/region4/29297/app/develop/faiss_distribute/"
    val  path_centreids = path_total+"centroids_vector"
    val path_adid_index = path_total+"adid_index"
    //val path_slave = path_total + "slave"

    //聚类中心个数和迭代次数
    val numClusters = 10
    val numIterations = 20

    //是否降维及降低的维度
    val pca_yes_or_not  = " "
    val pca_dimen_reduction_num = 1

    kmeans_cluster(sc,spark,path_total,path_centreids,path_adid_index,
      numClusters,numIterations,pca_yes_or_not,pca_dimen_reduction_num)

    //向master发送消息
//    val message_master = new message_public
//    message_master.pub_master()
//
//    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
//    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
//    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
//    println("the signal of hdfs ready is published!")

    sc.stop()
  }

  def pca(vector_only:RDD[Vector],adid_index_vector:RDD[(Int,Vector)],pca_dimen_reduction_num:Int): RDD[Vector] ={
    //创建pcaModel,将维度降低到8维
    @transient
    val pca = new PCA(pca_dimen_reduction_num).fit(vector_only)

    //为每个向量降维
    val adid_index_vector_pca = adid_index_vector.map{
      x=>
        val adid = x._1
        val vector_pca = pca.transform(x._2)
        (adid,vector_pca)
    }

    adid_index_vector_pca.map(_._2)
  }

  def centreids_vector_save(sc:SparkContext,centres_index:Array[(Int,Vector)],
                            path_centroids_vector:String,numClusters:Int): Unit ={
    sc.parallelize(centres_index).map{ x=>
      val centroids = x._1
      val centro_vector = x._2
      (centroids,centro_vector)
    }.repartition(1).saveAsTextFile(path_centroids_vector)

//    val slave_array = Array("10.102.23.107",
//    "10.102.23.108",
//    "10.102.23.109",
//    "10.102.23.110",
//    "10.102.23.111",
//    "10.102.23.112",
//    "10.102.23.113",
//    "10.102.23.114",
//    "10.102.23.115")
//    val numClusters = 3
//    val array_slave = ArrayBuffer[(Int,String)]()
//    for (i <- 0 until numClusters-1){
//      array_slave.append((i,slave_array(i)))
//    }
//
//    sc.parallelize(array_slave).map{
//      x=>
//        val centreids = x._1
//        val slave = x._2
//        (centreids,slave)
//    }.saveAsTextFile(path_slave)
  }

  def adid_index_and_save(adid_vector:RDD[(String,Vector)],path_adid_index:String): RDD[(String,Int)] = {

    val adid_index = adid_vector.map(_._1).zipWithIndex().map(x=>(x._1,x._2.toInt)).repartition(1)
    adid_index.saveAsTextFile(path_adid_index)

    adid_index
  }

  def get_data_from_hdfs(spark:SparkSession,sc:SparkContext): RDD[(String,Vector)] ={
    //获取需要聚类的数据，格式：adid：vector
    val data_from_source_file: BufferedSource = scala.io.Source.fromFile("item_emb_w")
    val adid_vector_lines_iterator = data_from_source_file.getLines().toList
    val adid_vector_lines_rdd = sc.parallelize(adid_vector_lines_iterator).repartition(200)


    //获取数据，转换成(adid：String,vector:Vector)这种数据格式
    val adid_vector = adid_vector_lines_rdd.map{ x =>
      val adid_and_vector = x.toString.split("\t")
      val adid = adid_and_vector(0)
      val adid_feature = adid_and_vector(1).split(",").map(x=>x.toDouble)
      val dense_vector = Vectors.dense(adid_feature)
      (adid,dense_vector)
    }

    adid_vector
  }

  def kmeans_cluster(sc:SparkContext,spark:SparkSession,path_total:String,
                     path_centreids:String,path_adid_index:String,
                     numClusters:Int,numIterations:Int,
                     pca_yes_or_not:String,pca_dimen_reduction_num:Int): Unit = {


    import spark.implicits._

    val adid_vector = get_data_from_hdfs(spark,sc)

    //为每个adid建立一个映射,格式：(adid,index)=>(String,Int),需要发送到主节点保存
    val adid_index = adid_index_and_save(adid_vector,path_adid_index)

    //得到adid的映射和向量之间的对应，格式(adid_index:Int,vector:Vector)
    val adid_index_vector = adid_index.join(adid_vector).map{x=>
      val adid_index = x._2._1
      val vector = x._2._2
      (adid_index,vector)
    }


    //获取向量RDD
    val vector_only = adid_index_vector.map(x=>x._2).cache()

    //决定聚类的向量是否降维,pca
    var vector_to_kmeans_cluster = vector_only
    if(pca_yes_or_not=="yes"){
      vector_to_kmeans_cluster = pca(vector_only,adid_index_vector,pca_dimen_reduction_num)
    }

    //KMeans训练,得到模型
    @transient
    val kmeans_model = KMeans.train(vector_to_kmeans_cluster,numClusters,numIterations)

    //得到聚类中心,格式数组：Array[Vector],并添加聚类中心的索引
    val centres_vector = kmeans_model.clusterCenters
    val centres_index = centres_vector.zipWithIndex.map(x=>(x._2,x._1))
    //将聚类中心及其索引，序列化后保存
    centreids_vector_save(sc,centres_index,path_centreids,numClusters)


    //预测每个向量对应的聚类中心的id
    val vector_adid_index_centreids = adid_index_vector.map{x=>
      val adid_index = x._1
      val vector = x._2
      val centreids = kmeans_model.predict(vector)
      (adid_index,centreids,vector.toString)
    }.toDF("adid_index","centreids","vector_string")

    //并按照聚类中心聚合groupby,聚合后再与原df拼接,得到完整数据表
    val group_by_centreids = vector_adid_index_centreids.groupBy("centreids")
    val vector_adid_index_centreids_group = group_by_centreids.count().join(vector_adid_index_centreids,"centreids")

    vector_adid_index_centreids
    println("******************************")
    print("============" + vector_adid_index_centreids.count() + "=================")
    println("******************************")


    //分别保存每个聚类中心下的向量数据，保存格式parquet
    for(centreid <- 0 until numClusters){
      vector_adid_index_centreids_group.filter($"centreids"===centreid).repartition(5).
        write.csv(path_total+s"centroids_$centreid")
        //.parquet(path_total+s"centroids_$centreid")
    }
//    val cluster_res_test = spark.read.load("/user/11085098/faiss/centroids_0")
//    cluster_res_test.show()
  }
}
