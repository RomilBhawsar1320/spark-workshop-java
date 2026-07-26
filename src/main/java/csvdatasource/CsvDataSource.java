package csvdatasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvDataSource {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("csv data source")
                .getOrCreate();

        Dataset<Row> df1 = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/products.csv");

//        Dataset<Row> df1 = spark.read()
//                .option("header", "true")
//                .option("inferSchema", "true")
//                .csv("src/main/resources/products.csv");


        df1.show(false);
        spark.stop();
    }
}
