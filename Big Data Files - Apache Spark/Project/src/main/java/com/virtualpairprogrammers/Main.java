package com.virtualpairprogrammers;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// -----------------------------------------------------------------------------------------------------------------------------------
		/*
		List<String> inputData = new ArrayList<>();
		
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("WARN: Tuesday 4 September 0406");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Friday 7 September 1854");
		inputData.add("WARN: Saturday 8 September 1942");
		*/
	    // -----------------------------------------------------------------------------------------------------------------------------------
		
		System.setProperty("hadoop.home.dir", "C:/hadoop");
	
		// To filer out Apache libraries logging.
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		// -----------------------------------------------------------------------------------------------------------------------------------
		// 55 - 58
		
		// Configuring Spark Session --> How we gonna operate on SparkSQL
		SparkSession spark = SparkSession.builder().appName("testingsql").master("local[*]").config("spark.sql.warehouse.dir","file:///c:/tmpl/").getOrCreate();
		
		// 78
		spark.conf().set("spark.sql.shuffle.partitions", "12");
		
		// Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		// Using filters on the dataset file.
		// Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007");
		
		// 59 - 62
		/*
		// Creating In Memory data.
		List<Row> inMemory = new ArrayList<Row>();
		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));
		
		// StructField --> Represent each column in our data structure.
		StructField[] fields = new StructField[] {
				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty())	
		};
		
		
		// Schema --> Tell spark what the datatypes of the columns are.
		StructType Schema = new StructType(fields);
		Dataset<Row> dataset = spark.createDataFrame(inMemory, Schema);
		*/
		
		// Reading big data file
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		
		SimpleDateFormat input = new SimpleDateFormat("MMMM");
		SimpleDateFormat output = new SimpleDateFormat("M");
		
		// 75 -> Using UDF in SparkSQL
		spark.udf().register("monthNum",(String month) -> {
			
			java.util.Date inputDate = input.parse(month);
			return Integer.parseInt(output.format(inputDate));
			
		} , DataTypes.IntegerType);
		
		// The time took to run this program is approximately 12 sec and it might take long to start the JVM.
		
		// 66 - 68
		// SQL version
		/*
		dataset.createOrReplaceTempView("logging_table");
		Dataset<Row> results =spark.sql("SELECT level,date_format(datetime,'MMMM') AS month, count(1) AS total FROM logging_table group by level,month ORDER BY monthNum(month), level");
		// We can improve this SQL syntax by changing the grouping, changing the order by. For this here we need to follow a query plan.
		// cast(first(date_format(datetime,'M')) AS int)
		results.show(100);
		*/
		
		// 80 - 81
		// Hash Aggregation Working
		// Reshaping the same way as the Java API version.

		dataset.createOrReplaceTempView("logging_table");
		
		Dataset<Row> results = spark.sql
		  ("select level, date_format(datetime,'MMMM') as month, count(1) as total, date_format(datetime,'M') as monthnum " + 
		   "from logging_table group by level, month, monthnum order by monthnum, level");	
		results = results.drop("monthnum");
		// Here it is using the Sort Aggregation.
		// Here date_format method on datetime will return a string eventhough we cast it to an int later on. (ex: "2", "12" etc). And the string type is not mutable.
		results.show(100);
		
		
		// 79 -> Explaining execution the plan
		results.explain();
		
		
		
		// 69 - 70
		// Java API version
		/*
		dataset = dataset.select(col("level"),functions.date_format(col("datetime"), "MMMM").alias("month"),
				                              functions.date_format(col("datetime"), ("M")).alias("monthnum").cast(DataTypes.IntegerType));
		dataset = dataset.groupBy("level","month","monthNum").count().as("total");
		dataset = dataset.drop("monthNum");
		dataset.show(100);
		*/
		
		// 79 -> Explaining the execution plan
		dataset.explain();
		
		
		// To look the results in Spark UI
		// To hold the consol in place.
		/*
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		*/
		
		/*
		Object[] months = new Object[] {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
		List<Object> columns = Arrays.asList(months);
		// List<Object> columns = new ArrayList<Object>();
		// columns.add("March");
		dataset = dataset.groupBy("level").pivot("month",columns).count().na().fill(0);
		*/
		
		// dataset = dataset.groupBy(col("level"),col("month"), col("monthnum")).count();
		// dataset = dataset.orderBy(col("monthnum"),col("level"));
		// dataset.show(100);
		
		// results.drop("monthnum");
		// results.show(100);
		/*
		results.createOrReplaceTempView("results_table");
		Dataset<Row> totals = spark.sql("select sum(total) from results_table");
		totals.show();
		*/
		/*
		results.createOrReplaceTempView("logging_table");
		results = spark.sql("select level, month,count(1) from logging_table group by level, month");
		*/
		
		// results.show(100);
		
		/*
		// Dataset<Row> modernArtResults = dataset.filter(row -> row.getAs("subject").equals("Modern Art") && Integer.parseInt(row.getAs("year")) >= 2007);
		
		Column subjectColumn = dataset.col("subject");
		Column yearColumn = dataset.col("year");
		Dataset<Row> modernArtResults = dataset.filter(subjectColumn.equalTo("Modern Art").and(yearColumn.geq(2007)));
		*/
		
		// Dataset<Row> modernArtResults = dataset.filter(col("subject").equalTo("Modern Art").and(col("year").geq(2007)));
		
		/*
		// Temporary View - Writing full SQL query.
		dataset.createOrReplaceTempView("my_students_table");
		Dataset<Row> results = spark.sql("SELECT distinct(year) FROM my_students_table ORDER BY year desc");
		results.show();
		*/
		
		// modernArtResults.show();
		
		/*
		// To show the records in the dataset.csv file.
		dataset.show();
		
		// Counting number of rows in the record.
		long numberOfRows = dataset.count();
		System.out.println("There are " + numberOfRows + " of records.");
		
		// To show the first row in the dataset.
		Row firstRow = dataset.first();
		System.out.println("First Row: " + firstRow);
		
		// To show first row first column.
		String subject = firstRow.get(2).toString();
		System.out.println("Subject: " + subject);
		
		// To show year column in the first row in the dataset.
		int year = Integer.parseInt(firstRow.getAs("year"));
		System.out.println("Year :" + year);
		*/
		
		spark.close();
		
		// Apache Configuration --> How we gonna work with JavaRDD's
		/*
		// Using Spark in local configuration we don't have a cluster.
		// SparkConf conf = new SparkConf().setAppName("startingspark").setMaster("local[*]");
		// Connecting to Spark Cluster.
		// JavaSparkContext sc = new JavaSparkContext(conf);
		*/
		
		
		
		
		// -----------------------------------------------------------------------------------------------------------------------------------
		// 32 - 34
		// Inner Join
		/*
		List <Tuple2<Integer,Integer>> visitsRows = new ArrayList<>();
		visitsRows.add( new Tuple2<>(4,18));
		visitsRows.add( new Tuple2<>(6,4));
		visitsRows.add( new Tuple2<>(10,9));
		
		List <Tuple2<Integer,String>> userRows = new ArrayList<>();
		userRows.add( new Tuple2<>(1,"John"));
		userRows.add( new Tuple2<>(2,"Bob"));
		userRows.add( new Tuple2<>(3,"Alan"));
		userRows.add( new Tuple2<>(4,"Doris"));
		userRows.add( new Tuple2<>(5,"Marybelle"));
		userRows.add( new Tuple2<>(6,"Raquel"));
		
		JavaPairRDD <Integer, Integer> visits = sc.parallelizePairs(visitsRows);
		JavaPairRDD <Integer, String> users = sc.parallelizePairs(userRows);
		
		// Inner Join
		// JavaPairRDD <Integer,Tuple2<Integer, String>> joinedRdd = visits.join(users);
		
		// Left Outer Join
		// JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRdd = visits.leftOuterJoin(users);
		// joinedRdd.collect().forEach( it -> System.out.println(it._2._2.orElse("blank").toUpperCase()));
		
		// Right Outer Join
		// JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinedRdd = visits.rightOuterJoin(users);
		// joinedRdd.collect().forEach( it -> System.out.println("User " + it._2._2 + " has " + it._2._1.orElse(0) + " visits."));
		
		// Full Join
		JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> joinedRdd = visits.fullOuterJoin(users);
	    joinedRdd.collect().forEach( it -> System.out.println("User " + it._1 + " " + it._2._2.orElse("Blank") + " : "+" has " + it._2._1.orElse(0) + " visits"));
		
		// Cartesian Joins
		// JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> joinedRdd = visits.cartesian(users);
		// joinedRdd.collect().forEach(System.out::println);
		// -----------------------------------------------------------------------------------------------------------------------------------
	    */
		
		
		// -----------------------------------------------------------------------------------------------------------------------------------
		// 21 - 25
		/*
		
		// Loading the text file stored in our computer.
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
		
		// Replacing anything thats not a letter with a blank.
		JavaRDD<String> lettersOnlyRdd = initialRdd.map( sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
		
		// Filtering out any line that is blank. (Also removing any trailing or leading white spaces.)
		JavaRDD<String> removeBlankLines = lettersOnlyRdd.filter( sentence -> sentence.trim().length() > 0);
		
		// Applying FlatMap -> Multiple outputs with one single input. (Converting sentences into individual words)
		JavaRDD<String> justWords = removeBlankLines.flatMap( sentence -> Arrays.asList(sentence.split(" ")).iterator());
		
		// Removing the blank words.
		JavaRDD<String> blankWordsRemoved = justWords.filter( word -> word.trim().length()>0);
		
		// Filtering out boring words.
		JavaRDD<String> justInterestingWords = blankWordsRemoved.filter( word -> Util.isNotBoring(word));
		
		// Applying ReduceByKey -> To count the instances
		JavaPairRDD<String,Long> pairRdd = justInterestingWords.mapToPair( word -> new Tuple2 <String,Long>(word,1L));
		JavaPairRDD<String,Long> totals = pairRdd.reduceByKey( (value1,value2) -> value1 + value2);
		
		// Switching and value pairs.
		JavaPairRDD<Long,String> switching = totals.mapToPair( tuple -> new Tuple2<Long,String>(tuple._2,tuple._1));
		
		// Sorting by Key.
		JavaPairRDD<Long,String> sorted = switching.sortByKey(false);
		
		System.out.println("There are " + sorted.getNumPartitions() + " partitions");
		
		// sorted = sorted.coalesce(1);
		
		// sorted.collect().forEach( element -> System.out.println(element));
		*/
		// -----------------------------------------------------------------------------------------------------------------------------------
		
		// List<Tuple2<Long,String>> results = sorted.take(10);
		// results.forEach(System.out::println);
		
		// FlatMap using Fluent APIs.
		// initialRdd.flatMap ( value -> Arrays.asList(value.split(" ")).iterator()).collect().forEach(System.out::println);
		
		/*
		// FlatMap --> Gives multiple outputs with the single input. Gives the flattened words from the sentences.
		JavaRDD<String> sentences = sc.parallelize(inputData);
		JavaRDD<String> words = sentences.flatMap( value -> Arrays.asList(value.split(" ")).iterator());
		
		// Filter Method --> To filter out unwanted words.
		JavaRDD<String> filteredWords = words.filter( word -> word.length() > 1);
		filteredWords.collect().forEach(System.out::println);
		*/
		
		/*
		// Grouping By Key version
		sc.parallelize(inputData).mapToPair( rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L)).groupByKey().foreach( tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances."));
		
		// Fluent API Version --> For single line of executable java code.
		// sc.parallelize(inputData).mapToPair( rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L)).reduceByKey((value1,value2) -> value1+value2).foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
		*/
		
		/* Fluent API Concept
		// JavaRDD --> To communicate with RDD.
		// Loading list data into RDD. [myRdd<->orginalIntegers]
		JavaRDD<String> orginalLogMessages = sc.parallelize(inputData);
		
		// Creating PairRDD --> To store key value pair. (Splitting and Mapping)
		JavaPairRDD<String,Long> pairRdd = orginalLogMessages.mapToPair( rawValue -> 
		{
			String[] columns = rawValue.split(":");
			String level = columns[0];
			String data = columns[1];
			
			return new Tuple2<>(level,1L);
		});
		
		// Reduce by key method. Keys are gathered together individually before the reduction method is applied.
		JavaPairRDD<String, Long> sumsRdd = pairRdd.reduceByKey( (value1,value2) -> value1+value2);
		
		sumsRdd.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
		*/
		
		/*
		// Reduce on RDD.
		Integer result = myRdd.reduce((value1,value2) -> value1 + value2);
		*/
		
		// Mapping on RDD. Returning the return value with IntegerWithSquareRoot instance.
		// JavaRDD<Tuple2<Integer, Double>> sqrtRdd = orginalIntegers.map(value -> new Tuple2<> (value,Math.sqrt(value)));
		
		////// IntegerWithSquareRoot iws = new IntergerWithSquareRoot(9);
		
		/*
		sqrtRdd.collect().forEach(System.out::println);
		
		System.out.println(result);
		
		// System.out.println(sqrtRdd.count());
		// Using Map and Reduce for counting number of items.
		JavaRDD<Long> singleInteger = sqrtRdd.map(value -> 1L);
		Long count = singleInteger.reduce((value1,value2) -> value1 + value2);
		System.out.println(count);
		*/
		
		// For unchecked exception.
		// sc.close();
	}
	
	

}
