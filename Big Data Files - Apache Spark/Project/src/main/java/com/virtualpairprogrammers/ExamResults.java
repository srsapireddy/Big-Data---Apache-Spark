package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

public class ExamResults {

	public static void main(String[] args) {
		
		
		// 75
		/*
		 * // This is just for reference, favour Java 8 lambdas
	private static UDF2<String, String,Boolean> hasPassedFunction = new UDF2<String, String, Boolean> () {

		@Override
		public Boolean call(String grade, String subject) throws Exception {
			
			if (subject.equals("Biology"))
			{
				if (grade.startsWith("A")) return true;
				return false;
			}
			
			return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
		}
		
	} ;
		 */
		System.setProperty("hadoop.home.dir", "C:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingsql").master("local[*]").config("spark.sql.warehouse.dir","file:///c:/tmpl/").getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		// 71
		// option("inferschema", true)
		// dataset = dataset.groupBy("subject").agg(max(col("score").cast(DataTypes.IntegerType)).alias("max score"), min(col("score").cast(DataTypes.IntegerType)).alias("min score"));
		// Column score = dataset.col("score");
		
		// 72
		// dataset = dataset.groupBy("subject").pivot("year").agg(round(avg(col("score")),2).alias("average"), round(stddev(col("score")),2).alias("stddev"));
		
		// 73
		// dataset = dataset.withColumn("pass", lit(col("grade").equalTo("A+")));
		
		// 74
		// UDF
		spark.udf().register("hasPassed", (String grade, String subject) -> {
		
			if (subject.equals("Biology"))
			{
				if (grade.startsWith("A")) return true;
			return false;
			
			} 
		return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
		}
		, DataTypes.BooleanType);
		dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject")));
		
		dataset.show();
	}
}

	
