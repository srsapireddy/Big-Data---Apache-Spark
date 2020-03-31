package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
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

public class HousePriceAnalysis {
	public static void main(String[] args) {
		
		// 95
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder()
				.appName("House Price Analysis")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.master("local[*]").getOrCreate();
		
		Dataset<Row> csvData = spark.read().option("inferSchema", true).option("header", true).csv("src/main/resources/kc_house_data.csv");
		
		// csvData.printSchema();
		// csvData.show();
		
		// 102, 106
				csvData = csvData.withColumn("sqft_above_percentage", col("sqft_above").divide(col("sqft_living"))).withColumnRenamed("price", "label");
				// csvData.show();
						
				// 96 --> Splitting Training Data with Random Splits
				Dataset<Row>[] dataSplits = csvData.randomSplit(new double[] {0.8,0.2});
				Dataset<Row> trainingAndTestData = dataSplits[0];
				Dataset<Row> holdOutData = dataSplits[1];
		
		// 105
				StringIndexer conditionIndexer = new StringIndexer();
				conditionIndexer.setInputCol("condition");
				conditionIndexer.setOutputCol("conditionIndex");
				// csvData = conditionIndexer.fit(csvData).transform(csvData);
				// csvData.show();
				
				StringIndexer gradeIndexer = new StringIndexer();
				gradeIndexer.setInputCol("grade");
				gradeIndexer.setOutputCol("gradeIndex");
				// csvData = gradeIndexer.fit(csvData).transform(csvData);
				
				StringIndexer zipcodeIndexer = new StringIndexer();
				zipcodeIndexer.setInputCol("zipcode");
				zipcodeIndexer.setOutputCol("zipcodeIndex");
				// csvData = zipcodeIndexer.fit(csvData).transform(csvData);
				
				OneHotEncoderEstimator encoder = new OneHotEncoderEstimator();
				encoder.setInputCols(new String[] {"conditionIndex","gradeIndex","zipcodeIndex"});
				encoder.setOutputCols(new String[] {"conditionVector","gradeVector","zipcodeVector"});
				// csvData = encoder.fit(csvData).transform(csvData);
				// csvData.show();
		
		VectorAssembler VectorAssembler = new VectorAssembler();
		VectorAssembler.setInputCols(new String[] {"bedrooms","bathrooms","sqft_living","sqft_lot","floors","sqft_above_percentage","conditionVector","gradeVector","zipcodeVector","waterfront"});
		VectorAssembler.setOutputCol("features");
		
		VectorAssembler VectorAssembler2 = new VectorAssembler();
		VectorAssembler.setInputCols(new String[] {"bedrooms","sqft_living","sqft_lot","floors","sqft_above_percentage","conditionVector","gradeVector","zipcodeVector","waterfront"});
		VectorAssembler.setOutputCol("features");
		
		
		/*
		Dataset<Row> csvDataWithFeatures = VectorAssembler.transform(csvData);
		Dataset<Row> modelInputData = csvDataWithFeatures.select("price","features").withColumnRenamed("price", "label");
		*/
		// modelInputData.show();
		
	

		// Dataset<Row> trainingData = trainingAndTestData[0];
		// Dataset<Row> testData = trainingAndTestData[1];
		
		// LinearRegressionModel model = new LinearRegression().fit(trainingData);
		// model.transform(testData).show();
		
		// 98 --> Setting Linear Regression Parameters
		LinearRegression linearRegression = new LinearRegression();
		ParamGridBuilder paramGridBuilder = new ParamGridBuilder();
		ParamMap[] paramMap = paramGridBuilder.addGrid(linearRegression.regParam(), new double[] {0.01,0.1,0.5}).addGrid(linearRegression.elasticNetParam(), new double[] {0,0.5,1}).build();
		
		// 99 --> Training, Test and Hold Data
		TrainValidationSplit trainValidationSplit = new TrainValidationSplit().setEstimator(linearRegression).setEvaluator(new RegressionEvaluator().setMetricName("r2")).setEstimatorParamMaps(paramMap).setTrainRatio(0.8);
		
		/*
		TrainValidationSplitModel model = trainValidationSplit.fit(trainingAndTestData);
		LinearRegressionModel lrModel = (LinearRegressionModel)model.bestModel();
		*/
		
		// 105 --> Instantiating the pipeline object
		Pipeline pipeline = new Pipeline();
		pipeline.setStages(new PipelineStage[] {conditionIndexer,gradeIndexer,zipcodeIndexer,encoder,VectorAssembler,trainValidationSplit});
		PipelineModel pipelineModel = pipeline.fit(trainingAndTestData);
		
		TrainValidationSplitModel model = (TrainValidationSplitModel)pipelineModel.stages()[5];
		LinearRegressionModel lrModel = (LinearRegressionModel)model.bestModel();
		
		Dataset<Row> holdOutResults = pipelineModel.transform(holdOutData);
		holdOutResults.show();
		holdOutResults = holdOutResults.drop("prediction");
		
		// 97 Accessing model accuracy with R2 and RMSE
		System.out.println("The training data R2 value is: " + lrModel.summary().r2() + " and the RMSE value is: " + lrModel.summary().rootMeanSquaredError());
		System.out.println("The test data R2 value is: " + lrModel.evaluate(holdOutResults).r2() + " and the RMSE value is: " + lrModel.evaluate(holdOutResults).rootMeanSquaredError());
		
		System.out.println("Coefficients: " + lrModel.coefficients() + " Intercept: " + lrModel.intercept());
		System.out.println("RegParam: " + lrModel.getRegParam() + " NetParam: " + lrModel.getElasticNetParam());
		
		
}
}