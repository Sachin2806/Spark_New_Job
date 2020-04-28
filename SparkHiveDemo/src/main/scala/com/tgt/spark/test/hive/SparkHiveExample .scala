package com.tgt.spark.test.hive

import java.io.File

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object SparkHiveExample  {
  
  def main(args: Array[String]){
    
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql("LOAD DATA LOCAL INPATH 'D:/CCA_175/kv.txt' INTO TABLE src")
    sql("SELECT * FROM src").show()
    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()
    
  }
 
   case class Record(key: Int, value: String)
}