package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;



import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

//102
import static org.apache.spark.sql.functions.col;

public class HousePriceFields {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder()
				.appName("House Price Analysis")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.master("local[*]").getOrCreate();
		
		Dataset<Row> csvData = spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/kc_house_data.csv");

		// 100
		//csvData.describe().show();
		
		// 102
		csvData = csvData.withColumn("sqft_above_percentage", col("sqft_above").divide(col("sqft_living")));
		
		// 101
		csvData = csvData.drop("id","date", "waterfront","view","condition","yr_renovated","zipcode","lat","long","sqft_above_percentage");
		
		for (String col : csvData.columns() ) {
			System.out.println("The correlation between the price and " + col + " is " + csvData.stat().corr("price", col));
		}
		
		// 102
		csvData = csvData.drop("sqft_lot","sqft_lot15","yr_build","sqft_living15");
		for(String col1: csvData.columns()) {
			for(String col2: csvData.columns()) {
				System.out.println("The correlation between " + col1 + " and " + col2 + " is " + csvData.stat().corr(col1, col1));
			}
		}
		
	}

}


