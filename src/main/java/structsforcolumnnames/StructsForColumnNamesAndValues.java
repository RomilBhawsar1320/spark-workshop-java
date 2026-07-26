package structsforcolumnnames;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class StructsForColumnNamesAndValues {

    public static void main(String[] args) {

        /*
         * SparkSession is the entry point to all Spark functionality.
         */
        SparkSession spark = SparkSession.builder()
                .appName("Structs For Column Names And Values")
                .master("local[*]")
                .getOrCreate();

        /*
         * Create input dataset.
         */
        Dataset<Row> ratings = createInputDataFrame(spark);

        System.out.println("\n===== INPUT DATA =====");
        ratings.show(false);

        /*
         * INPUT SCHEMA
         *
         * root
         *  |-- name: string
         *  |-- movieRatings: array
         *       |-- element: struct
         *             |-- movieName: string
         *             |-- rating: double
         *
         * Since movieRatings is an Array<Struct>,
         * we first flatten it using explode().
         */
        Dataset<Row> exploded = ratings
                .withColumn(
                        "movieRating",
                        explode(col("movieRatings"))
                )
                .select(
                        col("name"),
                        col("movieRating.movieName").alias("movieName"),
                        col("movieRating.rating").alias("rating")
                );

        System.out.println("\n===== AFTER EXPLODE =====");
        exploded.show(false);

        /*
         * Current Data:
         *
         * Manuel | Logan     | 1.5
         * Manuel | Zoolander | 3.0
         * Manuel | John Wick | 2.5
         *
         * John   | Logan     | 2.0
         * John   | Zoolander | 3.5
         * John   | John Wick | 3.0
         *
         * We want:
         *
         * Manuel | 1.5 | 3.0 | 2.5
         * John   | 2.0 | 3.5 | 3.0
         *
         * Therefore:
         * 1. Group by critic name
         * 2. Pivot on movie name
         * 3. Put rating inside corresponding movie column
         */
        Dataset<Row> solution = exploded
                .groupBy("name")
                .pivot("movieName")
                .agg(first("rating"));

        System.out.println("\n===== FINAL OUTPUT =====");
        solution.show(false);

        spark.stop();
    }

    /**
     * Creates the workshop input dataset.
     */
    private static Dataset<Row> createInputDataFrame(SparkSession spark) {

        /*
         * Schema for each element inside movieRatings array.
         *
         * Example:
         * {
         *   movieName = Logan,
         *   rating = 1.5
         * }
         */
        StructType movieRatingSchema = new StructType()
                .add("movieName", "string")
                .add("rating", "double");

        /*
         * Main schema.
         */
        StructType schema = new StructType()
                .add("name", "string")
                .add(
                        "movieRatings",
                        DataTypes.createArrayType(movieRatingSchema)
                );

        List<Row> rows = Arrays.asList(

                RowFactory.create(
                        "Manuel",
                        Arrays.asList(
                                RowFactory.create("Logan", 1.5),
                                RowFactory.create("Zoolander", 3.0),
                                RowFactory.create("John Wick", 2.5)
                        )
                ),

                RowFactory.create(
                        "John",
                        Arrays.asList(
                                RowFactory.create("Logan", 2.0),
                                RowFactory.create("Zoolander", 3.5),
                                RowFactory.create("John Wick", 3.0)
                        )
                )
        );

        return spark.createDataFrame(rows, schema);
    }
}