package com.tgt.spark.test.hive

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession

object Program5 {
  
  def main(args: Array[String]){
    
   val sparkSession  = SparkSession.builder()
                                   .appName("Spark Hive Example")
                                   .config("spark.master", "local")
                                   .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQnfL/Scala_DataFrames/spark-warehouse")
                                   .getOrCreate()
                            
                            
    val sc = sparkSession.sparkContext
    val sqlContext = new SQLContext(sc)
    
    System.out.println("INFO: ****************** Starting Connection HIVE ******************")
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkSession.sparkContext)
    sqlContext.sql("create database temp_db").show() 
    val hiveDF = hiveContext.sql("Show databases").show()

  }
}