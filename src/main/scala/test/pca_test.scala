package test

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.SparkSession

import scala.io.BufferedSource

object pca_test {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("faiss_kmeans").getOrCreate()
    val   sc  = spark.sparkContext
    pca(spark,sc)
    //pca_matrix(spark,sc)
    sc.stop()
  }

  def pca(spark:SparkSession,sc:SparkContext): Unit = {
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

    //获取adid的映射和向量，格式(adid_index:Int,vector:Vector)
    val adid_index = adid_vector.map(_._1).zipWithIndex().map(x=>(x._1,x._2.toInt))
//    val adid_index_vector = adid_index.join(adid_vector).map{x=>
//      val adid_index = x._2._1
//      val vector = x._2._2
//      LabeledPoint(adid_index,vector)
//    }

    //添加adid的索引
    val adid_index_vector = adid_index.join(adid_vector).map{x=>
      val adid_index = x._2._1
      val vector = x._2._2
      (adid_index,vector)
    }

    //获取向量RDD
    val vector_only_pca = adid_index_vector.map(_._2)
    //val vector_only_pca = adid_index_vector.map(_.features)


    //创建pcaModel,将维度降低到8维
    @transient
    val pca = new PCA(8).fit(vector_only_pca)

    //val adid_index_vector_pca = adid_index_vector.map(p => p.copy(features = pca.transform(p.features)))

    //为每个向量降维
    val adid_index_vector_pca = adid_index_vector.map{
      x=>
        val adid = x._1
        val vector_pca_8 = pca.transform(x._2)
        (adid,vector_pca_8)
    }

    println("@@@@@@@@@@@@@@@@@@")
    println("@@@@@@@@@@@@@@@@@@"+adid_index_vector_pca.first()+"@@@@@@@@@@@@@@@@@@")
    println("%%%%%%%%%%%%%%%%%%")

  }

  def pca_matrix(spark:SparkSession,sc:SparkContext): Unit ={
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

    val adid_index = adid_vector.map(_._1).zipWithIndex().map(x=>(x._1,x._2.toInt))
    val adid_index_vector = adid_index.join(adid_vector).map{x=>
      val adid_index = x._2._1
      val vector = x._2._2
      (adid_index,vector)
    }

    val vector_only_pca2 = adid_index_vector.map(_._2)

    val vector_matrix = new RowMatrix(vector_only_pca2)

    val vector_svd = vector_matrix.computePrincipalComponents(4)
    val vector_pca2_4 = vector_matrix.multiply(vector_svd)

    println("@@@@@@@@@@@@@@@@@@")
    println("@@@@@@@@@@@@@@@@@@"+vector_pca2_4+"@@@@@@@@@@@@@@@@@@")
    println("%%%%%%%%%%%%%%%%%%")

  }
}
