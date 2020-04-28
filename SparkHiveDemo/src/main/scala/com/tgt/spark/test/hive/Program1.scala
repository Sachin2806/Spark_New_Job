package com.tgt.spark.test.hive

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

object Program1 {
  
  def main(args: Array[String]){
    
    val spark = SparkSession.builder()
                            .appName("Spark Hive Example")
                            .config("spark.master", "local")
                            .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQnfL/Scala_DataFrames/spark-warehouse")
                            .enableHiveSupport()
                            .getOrCreate()
                            
    import spark.implicits._
    import spark.sql
    
    sql("CREATE TABLE IF NOT EXISTS employee(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    sql("LOAD DATA LOCAL INPATH 'C:/Users/CSC/Desktop/employee.txt' INTO TABLE employee")
    val result = sql("FROM employee SELECT id, name, age")
    result.show()
  }
}