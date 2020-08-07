import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class kafkaInputDemo {

    /**
     *https://spark.apache.org/docs/2.4.6/structured-streaming-kafka-integration.html
     */

    public static void main(String[] args) throws StreamingQueryException {
        SparkConf sparkConf = new SparkConf()
                .setAppName("app")
                .setMaster("local[*]");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        Map<String, String> optionMap = new HashMap<String, String>();
        // todo : your servers
        optionMap.put("kafka.bootstrap.servers", "xxxx");
        // todo : your topic
        optionMap.put("subscribe", "xxx");


        String column = "value";


        Dataset<Row> dataset = spark
                .read()
                .format("kafka")
                .options(optionMap)
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

                // todo : your column
                .select(column);


        dataset.show();


        // mathod 1

        List<String> stringList = dataset.as(Encoders.STRING()).collectAsList();

        String path = "e:/json/1.json";
        File f = new File(path);

        System.out.println(f);
        stringList.forEach(s -> {
            try {
                FileUtils.writeStringToFile(f, s + '\n', true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        Dataset<Row> ds = spark.read()
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .json("e:/json");

        ds.show();


//        // mathod 2
//
//        Dataset<String> stringDataset = dataset.toJSON();
//
//        stringDataset.show();
//
//
//        // example:
//        // {"value":"{\"id\":\"1\",\"float_num\":0.1234567}"} change to {"id":"1","float_num":0.1234567}
//
//        String path = "e:/json/1.json";
//        File f = new File(path);
//        stringDataset.foreach(s -> {
//            s = s.replace("\\", "");
//            String newString = s.substring(column.length() + 5, s.length() - 2);
//            FileUtils.writeStringToFile(f, newString + '\n', true);
//        });
//
//        Dataset<Row> ds = spark.read().json("e:/json");
//        ds.show();


    }

}
