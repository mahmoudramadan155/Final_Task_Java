package iti.Mahmoud;

import java.awt.*;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.clustering.BisectingKMeans;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
//import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;

import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.columnar.DOUBLE;
import org.knowm.xchart.*;
import org.knowm.xchart.internal.chartpart.Chart;
import org.knowm.xchart.style.Styler;
import scala.Array;
import scala.Tuple2;
import tech.tablesaw.api.Table;
import tech.tablesaw.plotly.Plot;
import tech.tablesaw.plotly.components.Figure;
import tech.tablesaw.plotly.components.Layout;
import tech.tablesaw.plotly.traces.PieTrace;

import static org.apache.spark.sql.functions.*;

public class App
{
    public static void main( String[] args ) throws IOException {
        String path = "src/main/resources/data/Wuzzuf_Jobs.csv";
        Logger.getLogger ("org").setLevel (Level.ERROR);

        final SparkSession sparkSession = SparkSession.builder ().appName("App").master("local[5]").getOrCreate ();

        final DataFrameReader dataFrameReader = sparkSession.read ();

        dataFrameReader.option ("header", "true");
        Dataset<Row> csvDataFrame = dataFrameReader.csv (path);
        csvDataFrame.printSchema ();

        csvDataFrame.createOrReplaceTempView("Mahmoud");

//        sparkSession.sql("SELECT * FROM Mahmoud").show();
//        System.out.println(csvDataFrame.count());
//        csvDataFrame.describe().show();
//
//        final Dataset<Row> Fristsql =sparkSession.sql ("select Company,count(distinct(Title)) from Mahmoud  GROUP BY Company  ORDER BY count(distinct(Title)) DESC ");
//        (Fristsql).show();
//        Fristsql.collectAsList().stream().forEach(System.out::println);
//        System.out.println(Fristsql.count());
//
//        final Dataset<Row> secondsql =sparkSession.sql ("select Title,count(Title) from Mahmoud  GROUP BY Title ORDER BY count(Title) DESC");
//        (secondsql).show();
//        secondsql.collectAsList().stream().forEach(System.out::println);
//        System.out.println(secondsql.count());
//
//        final Dataset<Row> thirdsql =sparkSession.sql ("select Location,count(Location) from Mahmoud  GROUP BY Location ORDER BY count(Location) DESC");
//        (thirdsql).show();
//        thirdsql.collectAsList().stream().forEach(System.out::println);
//        System.out.println(thirdsql.count());
//
//        final Dataset<Row> fourthsql =sparkSession.sql ("select Skills,count(Skills) from Mahmoud  GROUP BY Skills ORDER BY count(Skills) DESC");
//        (fourthsql).show();
//        fourthsql.collectAsList().stream().forEach(System.out::println);
//        System.out.println(fourthsql.count());

        csvDataFrame.na().drop();
        csvDataFrame.dropDuplicates();
//        csvDataFrame = csvDataFrame.filter(csvDataFrame.col("Title").isNotNull());
//        csvDataFrame = csvDataFrame.filter(csvDataFrame.col("Company").isNotNull());
//        csvDataFrame = csvDataFrame.filter(csvDataFrame.col("Location").isNotNull());
//        csvDataFrame = csvDataFrame.filter(csvDataFrame.col("Type").isNotNull());
//        csvDataFrame = csvDataFrame.filter(csvDataFrame.col("Level").isNotNull());
//        csvDataFrame = csvDataFrame.filter(csvDataFrame.col("YearsExp").isNotNull());
//        csvDataFrame = csvDataFrame.filter(csvDataFrame.col("Country").isNotNull());
//        csvDataFrame = csvDataFrame.filter(csvDataFrame.col("Skills").isNotNull());
        System.out.println(csvDataFrame.count());

        Dataset<Row> company = csvDataFrame.groupBy("Company").count().orderBy(desc("Count"));
        company.show();

        List<String> companys = company.select("Company").as(Encoders.STRING()).collectAsList();
        List<String> companysCount = company.select("Count").as(Encoders.STRING()).collectAsList();
        List<Integer> companysCount_int = companysCount.stream().map(s -> Integer.parseInt(s)).collect(Collectors.toList());


        PieChart topTenCompaniesChart = getChart(company.collectAsList().subList(0,20),"Top 20 Companies");
        new SwingWrapper<PieChart>(topTenCompaniesChart).displayChart();


        CategoryChart chart1 =
                new CategoryChartBuilder()
                        .width(1000).height(800).title("Top 20 popular job Company").xAxisTitle("Job Company").yAxisTitle("Popularity").build();

        chart1.addSeries("Company Counts", companys.subList(0, 20), companysCount_int.subList(0, 20));

        chart1.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart1.getStyler().setPlotGridLinesVisible(true);
        chart1.getStyler().setHasAnnotations(true);
        chart1.getStyler().setLegendVisible(false);
        chart1.getStyler().setStacked(true);
        chart1.getStyler().setXAxisLabelRotation(45);
        new SwingWrapper<>(chart1).displayChart();


        Dataset<Row> title_count = csvDataFrame.groupBy("Title").count().orderBy(desc("Count"));
        title_count.show();

        List<String> titles = title_count.select("Title").as(Encoders.STRING()).collectAsList();
        List<String> titlesCount = title_count.select("Count").as(Encoders.STRING()).collectAsList();
        List<Integer> titlesCount_int = titlesCount.stream().map(s -> Integer.parseInt(s)).collect(Collectors.toList());

        CategoryChart chart2 =
                new CategoryChartBuilder()
                        .width(1000).height(800).title("Top 20 popular job titles").xAxisTitle("Job title").yAxisTitle("Popularity").build();

        chart2.addSeries("Title Counts", titles.subList(0, 20), titlesCount_int.subList(0, 20));

        chart2.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart2.getStyler().setPlotGridLinesVisible(true);
        chart2.getStyler().setHasAnnotations(true);
        chart2.getStyler().setLegendVisible(false);
        chart2.getStyler().setStacked(true);
        chart2.getStyler().setXAxisLabelRotation(45);
        new SwingWrapper<>(chart2).displayChart();

        Dataset<Row> areas_count = csvDataFrame.groupBy("Country").count().orderBy(desc("Count"));
        areas_count.show();

        List<String> areas = areas_count.select("Country").as(Encoders.STRING()).collectAsList();
        List<String> areasCount = areas_count.select("Count").as(Encoders.STRING()).collectAsList();
        List<Integer> areasCount_int = areasCount.stream().map(s -> Integer.parseInt(s)).collect(Collectors.toList());

        CategoryChart chart3 =
                new CategoryChartBuilder()
                        .width(1000).height(800).title("Top 10 popular areas").xAxisTitle("Area").yAxisTitle("Popularity").build();

        chart3.addSeries("Areas Counts", areas.subList(0, 10), areasCount_int.subList(0, 10));

        chart3.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart3.getStyler().setPlotGridLinesVisible(true);
        chart3.getStyler().setHasAnnotations(true);
        chart3.getStyler().setLegendVisible(false);
        chart3.getStyler().setStacked(true);
        chart3.getStyler().setXAxisLabelRotation(45);

        new SwingWrapper<>(chart3).displayChart();


        Dataset<String> skills_col = csvDataFrame.select("Skills").as(Encoders.STRING());

        JavaRDD<String> skillsRdd = skills_col.toJavaRDD();
        JavaRDD<String> eachSkill = skillsRdd.flatMap(line -> Arrays.asList(line.split(",")).iterator());

        JavaPairRDD<String, Integer> skillPairRdd = eachSkill.mapToPair(skill -> new Tuple2<>(skill, 1));
        JavaPairRDD<String, Integer> countSkills = skillPairRdd.reduceByKey((x, y) -> x + y);
        JavaPairRDD<Integer, String> skill_count_pairs = countSkills.mapToPair(item -> new Tuple2<>(item._2(), item._1())).sortByKey(false);

        System.out.println("Most important skills required:");
        for (Tuple2<Integer, String> skillCount : skill_count_pairs.collect()) {
            System.out.println(skillCount._2() + " --> " + skillCount._1());
        }


        StringIndexer indexer = new StringIndexer().setInputCol("YearsExp").setOutputCol("Factorized YearsExp");
        indexer.fit(csvDataFrame).transform(csvDataFrame).show();


//        csvDataFrame.select("Title","Company","Location","Type","Level").show(10);
//         csvDataFrame.as(Encoders.STRING()).collectAsList()

//        System.out.println(csvDataFrame.takeAsList(csvDataFrame.map()).stream().count())

//        Dataset<String> namesDS = csvDataFrame.map((MapFunction<Row, String>) row -> row.getString(0),Encoders.STRING());
//        namesDS.show();


//        List<String> title = csvDataFrame.toJavaRDD().map(new Function<Row, String>() {
//            public String call(Row row) {
//                return row.getAs(0).toString();
//            }
//        }).collect();
//
////        System.out.println(title);
//        List<String> company = csvDataFrame.toJavaRDD().map(new Function<Row, String>() {
//            public String call(Row row) {
//                return row.getAs("Company").toString();
//            }
//        }).collect();
////        System.out.println(company);
//
//        Dataset<String> Title = csvDataFrame.map(new MapFunction<Row, String>() {
//            @Override
//            public String call(Row row) throws Exception {
//                return "Key: " + row.get(1);
//            }
//        }, Encoders.STRING());
////        Title.show();
//
//        Dataset<String> Company = csvDataFrame.map(new MapFunction<Row, String>() {
//            @Override
//            public String call(Row row) throws Exception {
//                return "Key: " + row.get(1);
//            }
//        }, Encoders.STRING());
//        Company.show();

/*
        StringIndexer indexer1 = new StringIndexer().setInputCol("Title").setOutputCol("Factorized Title");
        indexer1.fit(csvDataFrame).transform(csvDataFrame);

        Dataset<Row> kmean_test = csvDataFrame.select("Factorized Title","Factorized YearsExp");

        StringIndexer indexer2 = new StringIndexer().setInputCol("Factorized YearsExp").setOutputCol("features");
        Dataset<Row> indexed_title = indexer2.fit(kmean_test).transform(kmean_test);
        VectorAssembler vectorAssembler = new VectorAssembler();
        String inputColumns[] = {"features"};
        vectorAssembler.setInputCols(inputColumns);
        vectorAssembler.setOutputCol("clusters");
        Dataset<Row> dataset = vectorAssembler.transform(indexed_title);
        dataset.show();
        KMeans kmeans = new KMeans().setK(2).setSeed(1L);

        // KMeans for  job titles
        KMeansModel model = kmeans.fit(kmean_test);
        // Make predictions
        Dataset<Row> predictions = model.transform(dataset);

        // Evaluate clustering by computing Silhouette score
        ClusteringEvaluator evaluator = new ClusteringEvaluator();
        double silhouette = evaluator.evaluate(predictions);
        System.out.println("Silhouette with squared euclidean distance = " + silhouette);
        // Shows the result.
        Vector[] centeroids = (Vector[]) model.clusterCenters();
        System.out.println("Cluster Centeroids: ");
        for (Vector centeroid : centeroids) {
            System.out.println(centeroid);
        }

*/


//        Dataset<Row>[] splits = csvDataFrame.randomSplit(new double[] { 0.8, 0.2 },42);
//        Dataset<Row> trainingFeaturesData = splits[0];
//        Dataset<Row> testFeaturesData = splits[1];

//        Dataset<Row> Fetuer =csvDataFrame.select("Title").withColumn("features",array("Title"));
//        BisectingKMeans bkm = new BisectingKMeans().setK(10).setSeed(1);
//        BisectingKMeansModel model = bkm.fit(Fetuer);
//
//// Make predictions
//        Dataset<Row> predictions = model.transform(csvDataFrame);
//
//// Evaluate clustering by computing Silhouette score
//        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
//
//        double silhouette = evaluator.evaluate(predictions);
//        System.out.println("Silhouette with squared euclidean distance = " + silhouette);
//
//// Shows the result.
//        System.out.println("Cluster Centers: ");
//        Vector[] centers = (Vector[]) model.clusterCenters();
//        for (Vector center : centers) {
//            System.out.println(center);
//        }

    }

    public static PieChart getChart(List<Row> rows, String name) {

        PieChart chart = new PieChartBuilder().width(800).height(600).title(name).build();

        for (int i = 0;i< rows.size();i++)
        {
            chart.addSeries((String)rows.get(i).get(0),(Long)rows.get(i).get(1));
        }


        return chart;
    }

}