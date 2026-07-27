package ReverseEngineerShowUsingCSV;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.trim;

public class ReverseEngineerShowUsingCSV {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Reverse Engineer Show Output")
                .getOrCreate();

        Dataset<Row> rawDf = spark.read()
                .option("header", "true")
                .option("delimiter", "|")
                .option("comment", "+")
                .csv("src/main/resources/show-output.txt");

        System.out.println("Raw Data:");

        rawDf.show(false);

        Dataset<Row> result = rawDf.select(
                trim(col(" id")).alias("id"),
                trim(col("             Text1")).alias("Text1"),
                trim(col("Text2")).alias("Text2")
        );

        System.out.println("Reverse Engineered Dataset:");

        result.show(false);

        result.printSchema();

        spark.stop();
    }
}