package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

public class VPPFreeTrials {
	
	public static UDF1<String,String> countryGrouping = new UDF1<String,String>() {

		@Override
		public String call(String country) throws Exception {
			List<String> topCountries =  Arrays.asList(new String[] {"GB","US","IN","UNKNOWN"});
			List<String> europeanCountries =  Arrays.asList(new String[] {"BE","BG","CZ","DK","DE","EE","IE","EL","ES","FR","HR","IT","CY","LV","LT","LU","HU","MT","NL","AT","PL","PT","RO","SI","SK","FI","SE","CH","IS","NO","LI","EU"});
			
			if (topCountries.contains(country)) return country; 
			if (europeanCountries .contains(country)) return "EUROPE";
			else return "OTHER";
		}
		
	};
	
	public static void main(String[] args) {

		
	System.setProperty("hadoop.home.dir", "c:/hadoop");
	Logger.getLogger("org.apache").setLevel(Level.WARN);

	SparkSession spark = SparkSession.builder()
			.appName("VPP Chapter Views")
			.config("spark.sql.warehouse.dir","file:///c:/tmp/")
			.master("local[*]").getOrCreate();
	
	Dataset<Row> csvData = spark.read().option("inferSchema", true).option("header", true).csv("src/main/resources/VPPFreeTrials.csv");

	spark.udf().register("countryGrouping", countryGrouping,DataTypes.StringType);
	
	// Labels must be numbers but with the decision trees the labels are categories.
    // 1 -> paying customer, 0 -> non-paying customer
	csvData = csvData.withColumn("country", callUDF("countryGrouping",col("country"))).withColumn("label", when(col("payments_made").geq(1),lit(1)).otherwise(lit(0)));
	
	StringIndexer countryIndexer = new StringIndexer();
	csvData = countryIndexer.setInputCol("country").setOutputCol("countryIndex").fit(csvData).transform(csvData);
	
	Dataset<Row> countryIndex = csvData.select("countryIndex");
	// countryIndex.show();
	
	IndexToString indexToString = new IndexToString();
	indexToString.setInputCol("countryIndex").setOutputCol("value").transform(countryIndex).show();
	
	VectorAssembler vectorAssembler = new VectorAssembler();
	vectorAssembler.setInputCols(new String [] {"countryIndex","rebill_period","chapter_access_count","seconds_watched"});
	vectorAssembler.setOutputCol("features");
	
	Dataset<Row> inputData = vectorAssembler.transform(csvData).select("label","features");
	// inputData.show();
	
	Dataset<Row> [] trainingAndHoldOutData = inputData.randomSplit(new double[] {0.8,0.2});
	Dataset<Row> trainingData = trainingAndHoldOutData[0];
	Dataset<Row> holdOutData = trainingAndHoldOutData[1];
	
	// Building the model
	// MaxDepth -> How many branches we want in our tree
	DecisionTreeClassifier dtClassifier = new DecisionTreeClassifier();
	dtClassifier.setMaxDepth(3);
	
	DecisionTreeClassificationModel model = dtClassifier.fit(trainingData);
	Dataset<Row> predictions = model.transform(holdOutData);
	predictions.show();
	
	System.out.println(model.toDebugString());
	
	MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
	evaluator.setMetricName("accuracy");
	System.out.println("The accuracy of the model is " + evaluator.evaluate(predictions));
	
	
	
}
}
