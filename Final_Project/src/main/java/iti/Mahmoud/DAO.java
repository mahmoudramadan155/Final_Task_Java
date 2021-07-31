package iti.Mahmoud;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DAO {
    public static void main(String[] args) {


        List<POJO> country = new ArrayList<>();

        SparkConf conf = new SparkConf ().setAppName ("DAO").setMaster ("local[5]");
        JavaSparkContext sparkContext = new JavaSparkContext (conf);

        JavaRDD<POJO> jobPOJO = sparkContext.textFile("src/main/resources/Wuzzuf_Jobs.txt")
                .map(new Function<String, POJO>() {
                    @Override
                    public POJO call(String line) throws Exception {
                        String[] parts = line.split(",");
                        POJO person = new POJO();
                        person.setTitle(parts[0]);
                        person.setCompany(parts[1]);
                        person.setLocation(parts[2]);
                        person.setType(parts[3]);
                        person.setLevel(parts[4]);
                        person.setYearExp(parts[5]);
                        person.setCountry(parts[6]);
                        person.setSkills(parts[7]);
                        return person;
                    }
                });

        POJO p =new POJO();
        System.out.println(p.getCompany());

//        Dataset<Row> peopleDF =.createDataFrame(jobPOJO, POJO.class);
        }

    }











//public static JavaRDD createSparkLoadData() throws IOException {
//    SparkConf conf = new SparkConf ().setAppName ("wordCounts").setMaster ("local[3]");
//
//    JavaSparkContext sparkContext = new JavaSparkContext (conf);
//    JavaRDD<String> content = sparkContext.textFile ("src/main/resources/data/Wuzzuf_Jobs.csv");
//    return content;
//}
//
//    public static String extractTitle(String videoLine) {
//        try {
//            return videoLine.split (",")[2];
//        } catch (ArrayIndexOutOfBoundsException e) {
//            return "";
//        }
//}

/*
    private static POJO createPojo(JavaRDD content){
        JavaRDD<String> titles = content
                .map (YoutubeTitleWordCount::extractTitle)
                .filter (StringUtils::isNotBlank);
        JavaRDD<String> words = titles.flatMap (title -> Arrays.asList (title
                .toLowerCase ()
                .trim ()
                .replaceAll ("\\p{Punct}", "")
                .split (" ")).iterator ());
        // COUNTING
        Map<String, Long> wordCounts = words.countByValue ();
        List<Map.Entry> sorted = wordCounts.entrySet ().stream ()
                .sorted (Map.Entry.comparingByValue ()).collect (Collectors.toList ());
        return new POJO();
}
*/

