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
    //sql("CREATE TABLE IF NOT EXISTS employee(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    //sql("Show tables").show()
    //sql("LOAD DATA LOCAL INPATH 'C:/Users/CSC/Desktop/employee.txt' INTO TABLE employee")
    //val result = sql("FROM employee SELECT id, name, age")
    //result.show()
    
    }
}