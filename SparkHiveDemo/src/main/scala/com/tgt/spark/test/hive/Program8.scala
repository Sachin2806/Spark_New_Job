package com.tgt.spark.test.hive

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession 

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Program8 {
  
  def main(args: Array[String]){
   
  val spark = SparkSession.builder()
                            .appName("Spark Hive Example")
                            .master("local[*]")
                            .config("spark.sql.warehouse.dir", "file:C:/Users/CSC/Project_Space/SparkHiveDemo/spark-warehouse")
                            .config("spark.sql.warehouse.dir", "file:/C:/Windows/system32/spark-warehouse")
                            .enableHiveSupport()
                            .getOrCreate();
    
    
    import spark.implicits._             
    import spark.sql
    
    val sc = spark.sparkContext
    val sqlContext = new SQLContext(sc)
    
    System.out.println("INFO: ****************** Starting Connection HIVE ******************")
    val testdf = Seq(("Word1", 1), ("Word4", 4), ("Word8", 8)).toDF;
    testdf.show;
    testdf.write.mode("overwrite").saveAsTable("WordCount");
    
    }
}