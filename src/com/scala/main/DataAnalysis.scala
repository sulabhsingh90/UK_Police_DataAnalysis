
/*
 * Dataset used in this exaple is from https://data.police.uk/data/open-data/
 * It is available under Open Government Licence v3.0.
 * https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/
 * 
 * */

package com.scala.main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc,desc,count,expr,sum,when,col,lit,bround}
object DataAnalysis {

	def main(args:Array[String]){
		val spark=SparkSession.builder().appName("DataAnalysis").master("local[*]").getOrCreate()
				val ukPoliceDF=spark.read.format("csv").option("header", "true").option("mode", "FAILFAST")
				.option("inferSchema", "true")
				.option("path", "/UKPolice_data/*")
				.load()

				/*
				 * Schema for this dataframe is infered from header that is looked like
				 * StructType(StructField(Crime ID,StringType,true), StructField(Month,StringType,true), StructField(Reported by,StringType,true), StructField(Falls within,StringType,true), StructField(Longitude,DoubleType,true), StructField(Latitude,DoubleType,true), StructField(Location,StringType,true), StructField(LSOA code,StringType,true), StructField(LSOA name,StringType,true), StructField(Crime type,StringType,true), StructField(Last outcome category,StringType,true), StructField(Context,StringType,true))
				 */

				ukPoliceDF.select("Last outcome category").distinct().collect().foreach(println)
				val topCrime=ukPoliceDF.groupBy(col("crime type"))
				.count().withColumnRenamed("count", "total")
				.orderBy(desc("total"))

				topCrime.collect().foreach(println)

				val notGuilty=ukPoliceDF
				.groupBy("crime type").agg(count("*").alias("total"),
						sum(when(col("Last outcome category")==="Defendant found not guilty",lit(1)).otherwise(lit(0))
								).alias("notguilty")).withColumn("notguiltypercent", bround(col("notguilty")/col("total")*lit(100),2)).orderBy(desc("notguilty"),desc("total"))

				notGuilty.collect().foreach(println)		

	}
}