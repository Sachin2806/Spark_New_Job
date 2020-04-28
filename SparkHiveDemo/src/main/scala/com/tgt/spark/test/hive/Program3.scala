package com.tgt.spark.test.hive

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession

object Program3 {
  
  def main(args: Array[String]){
    
   val sparkSession  = SparkSession.builder()
                                   .appName("Spark Hive Example")
                                   .config("spark.master", "local")
                                   .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQnfL/Scala_DataFrames/spark-warehouse")
                                   .enableHiveSupport()
                                   .getOrCreate()
                                                    
    val sc = sparkSession.sparkContext
    val sqlContext = new SQLContext(sc)
    
    System.out.println("INFO: ****************** Starting Connection HIVE ******************")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS employee(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    sqlContext.sql("Show tables").show()
    //sqlContext.sql("LOAD DATA LOCAL INPATH 'C:/Users/CSC/Desktop/employee.txt' INTO TABLE employee")
    //val result = sqlContext.sql("FROM employee SELECT id, name, age")
    //result.show()
  }
}