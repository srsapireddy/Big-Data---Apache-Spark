package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class VPPChapterViewsLogistic {

public static void main(String[] args) {
		
		// 95
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder()
				.appName("VPP Chapter Views")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.master("local[*]").getOrCreate();
		
		Dataset<Row> csvData = spark.read().option("inferSchema", true).option("header", true).csv("src/main/resources/vppChapterViews/*.csv");

		// csvData.show();
		// Filtering out all the records where cancel is set to true
		csvData = csvData.filter("is_cancelled = false").drop("observation_date","is_cancelled");
		
		// Label
		// 1 - customers watched no videos, 0 - customers watched some videos
		
		// Getting rid of nulls
		csvData = csvData.withColumn("firstSub", when(col("firstSub").isNull(),0).otherwise(col("firstSub")))
				.withColumn("all_time_views", when(col("all_time_views").isNull(),0).otherwise(col("all_time_views")))
						.withColumn("last_month_views", when(col("last_month_views").isNull(),0).otherwise(col("last_month_views")))
								.withColumn("next_month_views", when(col("next_month_views").$greater(0),0).otherwise(1));
		
		// Rename the field that is our label (next_month_views) column
		csvData = csvData.withColumnRenamed("next_month_views", "label");
		
		// Encoding category columns
		StringIndexer payMethodIndexer = new StringIndexer();
		csvData = payMethodIndexer.setInputCol("payment_method_type").setOutputCol("payIndex").fit(csvData).transform(csvData);
		StringIndexer countryIndexer = new StringIndexer();
		csvData = countryIndexer.setInputCol("country").setOutputCol("countryIndex").fit(csvData).transform(csvData);
		StringIndexer periodIndexere = new StringIndexer();
		csvData = periodIndexere.setInputCol("rebill_period_in_months").setOutputCol("periodIndex").fit(csvData).transform(csvData);
		
		OneHotEncoderEstimator encoder = new OneHotEncoderEstimator();
		csvData = encoder.setInputCols(new String [] {"payIndex","countryIndex","periodIndex"}).setOutputCols(new String [] {"payVector","countryVector","periodVector"}).fit(csvData).transform(csvData);
		
		// Creating the final vector of features
		VectorAssembler vectorAssembler = new VectorAssembler();
		Dataset<Row> inputData = vectorAssembler.setInputCols(new String [] {"firstSub","all_time_views","last_month_views","payVector","countryVector","periodVector"}).setOutputCol("features").transform(csvData).select("label","features");
		inputData.show();
		
		// Building Model
		Dataset<Row>[] trainAndHoldOutData = inputData.randomSplit(new double[] {0.9,0.1});
		Dataset<Row> trainAndTestData = trainAndHoldOutData[0];
		Dataset<Row> HoldData = trainAndHoldOutData[1];
		
		LogisticRegression logisticRegression = new LogisticRegression();
		ParamGridBuilder pgb = new ParamGridBuilder();
		ParamMap[] paramMap = pgb.addGrid(logisticRegression.regParam(), new double[] {0.01,0.1,0.3,0.5,0.7,1.0}).addGrid(logisticRegression.elasticNetParam(), new double[] {0,0.5,1}).build();
		
		TrainValidationSplit tvs = new TrainValidationSplit();
		tvs.setEstimator(logisticRegression).setEvaluator(new RegressionEvaluator().setMetricName("r2")).setEstimatorParamMaps(paramMap).setTrainRatio(0.9);
				
		TrainValidationSplitModel model = tvs.fit(trainAndTestData);
		
		LogisticRegressionModel lrModel = (LogisticRegressionModel)model.bestModel();
		System.out.println("The value of the parameter r2 is: " + lrModel.summary().accuracy());
		
		System.out.println("coefficients: " + lrModel.coefficients() + " intercept " + lrModel.intercept());
		System.out.println("reg param: " + lrModel.getRegParam() + " elastic net param: " + lrModel.getElasticNetParam());
		
		double truePositives = lrModel.evaluate(HoldData).truePositiveRateByLabel() [1];
		double falsePositives = lrModel.evaluate(HoldData).falsePositiveRateByLabel() [0];
		
		System.out.println("For hold out data the likelyhood of being correct is: " + (truePositives/(truePositives+falsePositives)));
		System.out.println("The holdout data accuracy is: " + lrModel.evaluate(HoldData).accuracy());
		
		lrModel.transform(HoldData).groupBy("label","prediction").count().show();
		
		
		
		
}
}

