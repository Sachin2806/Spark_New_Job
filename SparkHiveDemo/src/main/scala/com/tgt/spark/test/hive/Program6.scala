package com.tgt.spark.test.hive

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession 


object Program6 {
  
  def main(args: Array[String]){
   
  
    val spark  = SparkSession
                 .builder()
                 .appName("Spark Hive Example")
                 .config("spark.master", "local")
                 .config("spark.sql.warehouse.dir", "file:/C:/Windows/system32/spark-warehouse")
                 .enableHiveSupport()
                 .getOrCreate()
    
    import spark.implicits._             
    import spark.sql
    
    System.out.println("INFO: ****************** Starting Connection HIVE ******************")
    sql("create database retail_db").show()
    sql("Show databases").show()
    
    }
}