package collectset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class CollectSetStandardFunction {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("collect set standard function")
                .getOrCreate();

        Dataset<Row> df1 = spark.range(50).withColumn( "key",col("id").mod(5));
        df1.show();

        /*
         * Group by key
         * collect_set(id)
         * collects unique ids into an array.
         */
        Dataset<Row> df2 = df1.groupBy("key").agg(collect_set("id").alias("all"));
        df2.show(false);
        /*
         * slice(array, start, length)
         * start = 1
         * length = 3
         * Spark arrays use 1-based indexing.
         * This returns only the first 3 elements
         * from the collected array.
         */
        Dataset<Row> solution = df2.withColumn("only_first_three",slice(col("all"),1,3));
        solution.show(false);

    }
}
