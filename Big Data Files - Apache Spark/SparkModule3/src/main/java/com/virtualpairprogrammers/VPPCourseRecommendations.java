package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

import java.util.List;

public class VPPCourseRecommendations {

	public static void main(String[] args) {

		// Recommender System to recommend the courses on previous watch list of courses
		
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder()
				.appName("VPP Chapter Views")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.master("local[*]").getOrCreate();

		Dataset<Row> csvData = spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/VPPcourseViews.csv");
		
		csvData = csvData.withColumn("proportionWatched", col("proportionWatched").multiply(100));
		
		//csvData.groupBy("userId").pivot("courseId").sum("proportionWatched").show();
				
		ALS als = new ALS()
				.setMaxIter(10)
				.setRegParam(0.1)
				.setUserCol("userId")
				.setItemCol("courseId")
				.setRatingCol("proportionWatched");
		
		ALSModel model = als.fit(csvData);
		
		model.setColdStartStrategy("drop");
		
		Dataset<Row> userRecs = model.recommendForAllUsers(5);

		List<Row> userRecsList = userRecs.takeAsList(5);
		
		for (Row r : userRecsList) {
			int userId = r.getAs(0);
			String recs = r.getAs(1).toString();
			System.out.println("User " + userId + " we might want to recommend " + recs);
			System.out.println("This user has already watched: ");
			csvData.filter("userId = " + userId).show();
		}
		
		/*
		Dataset<Row> testData = spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/VPPcourseViewsTest.csv");
		
		model.transform(testData).show();
		model.recommendForUserSubset(testData, 5).show();
	    */
	}
}
