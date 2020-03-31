package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymCompetitorsClustering {
	public static void main(String[] args) {
		
		// 90
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder()
				.appName("Gym Competitors")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.master("local[*]").getOrCreate();
		
		Dataset<Row> csvData = spark.read().option("inferSchema", true).option("header", true).csv("src/main/resources/GymCompetition.csv");
		csvData.show();
		// csvData.printSchema();
		
		// 104
		StringIndexer genderIndexer = new StringIndexer();
		genderIndexer.setInputCol("Gender");
		genderIndexer.setOutputCol("GenderIndex");
		csvData = genderIndexer.fit(csvData).transform(csvData);
		// csvData.show();
		
		OneHotEncoderEstimator genderEncoder = new OneHotEncoderEstimator();
		genderEncoder.setInputCols(new String[] {"GenderIndex"});
		genderEncoder.setOutputCols(new String[] {"GenderVector"});
		csvData = genderEncoder.fit(csvData).transform(csvData);
		// csvData.show();
		
		VectorAssembler vectorAssembler = new VectorAssembler();
		Dataset<Row> inputData = vectorAssembler.setInputCols(new String [] {"GenderVector","Age","Height","Weight","NoOfReps"}).setOutputCol("features").transform(csvData).select("features");
		// inputData.show();
		
		// Building model
		KMeans kMeans = new KMeans();
		kMeans.setK(3);
		
		KMeansModel model = kMeans.fit(inputData);
		Dataset<Row> predictions = model.transform(inputData);
		predictions.show();
		
		// Cluster centers for each cluster
		Vector[] clusterCenter = model.clusterCenters();
			for(Vector v: clusterCenter) {System.out.println(v);}
			
		// How many competitors will end up in each of the groups. Is it a good spread or not?
		predictions.groupBy("prediction").count().show();	
		
		// Model Accuracy
		// Calculating the Sum of Squares
		System.out.println("The SEE is: " + model.computeCost(inputData));
		
		// Slihouette with squared euclidean distance
		ClusteringEvaluator evaluator = new ClusteringEvaluator();
		System.out.println("Slihouette with squared euclidean distance is " + evaluator.evaluate(predictions));

}
	}

