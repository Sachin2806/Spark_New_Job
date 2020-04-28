package com.tgt.spark.test.hive

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession

object Program4 {
  
  def main(args: Array[String]){
    
   val sparkSession  = SparkSession.builder()
                                   .appName("Spark Hive Example")
                                   .config("spark.master", "local")
                                   .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQnfL/Scala_DataFrames/spark-warehouse")
                                   .getOrCreate()
                            
                            
    val sc = sparkSession.sparkContext
    val sqlContext = new SQLContext(sc)
    
    System.out.println("INFO: ****************** Starting Connection HIVE ******************")
    sqlContext.sql("create database sachin_db").show()
    sqlContext.sql("Show databases").show()
  }
}