

import org.apache.spark.sql.SparkSession;

public class HelloSpark {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("HelloSpark")
                .master("local[*]")
                .getOrCreate();

        spark.range(10).show();

        spark.stop();
    }
}