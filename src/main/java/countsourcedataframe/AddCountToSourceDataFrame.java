package countsourcedataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;

public class AddCountToSourceDataFrame {

    public static void main(String[] args) {

        /*
         * SparkSession is the entry point to Spark.
         *
         * Every Spark application starts by creating a SparkSession.
         *
         * local[*]
         *  - Runs Spark locally on the current machine.
         *  - Uses all available CPU cores.
         *
         * This is suitable for workshop exercises and local development.
         */
        SparkSession spark = SparkSession.builder()
                .appName("Add Count To Source DataFrame")
                .master("local[*]")
                .getOrCreate();

        /*
         * Creating sample input data.
         *
         * We have two unique groups:
         *
         * Group 1:
         * IP1.54880 -> IP2.5001
         * label = 2
         *
         * Group 2:
         * IP2.5001 -> IP1.54880
         * label = 2
         *
         * Goal:
         * Count how many rows belong to each group and add that count
         * to every record of the corresponding group.
         */
        List<Row> rows = Arrays.asList(
                RowFactory.create("05:49:56.604899", "IP1.54880", "IP2.5001", 2),
                RowFactory.create("05:49:56.604900", "IP1.54880", "IP2.5001", 2),
                RowFactory.create("05:49:56.604899", "IP1.54880", "IP2.5001", 2),
                RowFactory.create("05:49:56.604900", "IP1.54880", "IP2.5001", 2),
                RowFactory.create("05:49:56.604899", "IP1.54880", "IP2.5001", 2),
                RowFactory.create("05:49:56.604908", "IP2.5001", "IP1.54880", 2),
                RowFactory.create("05:49:56.604908", "IP2.5001", "IP1.54880", 2),
                RowFactory.create("05:49:56.604908", "IP2.5001", "IP1.54880", 2)
        );

        /*
         * Define schema explicitly.
         *
         * Spark needs to know:
         * - column names
         * - column data types
         *
         * Schema:
         * column0 -> Time
         * column1 -> Source IP and Port
         * column2 -> Destination IP and Port
         * label   -> Classification Label
         */
        StructType schema = new StructType()
                .add("column0", "string")
                .add("column1", "string")
                .add("column2", "string")
                .add("label", "integer");

        /*
         * Convert Java collection into Spark DataFrame.
         *
         * DataFrame is Spark's distributed tabular data structure.
         */
        Dataset<Row> input = spark.createDataFrame(rows, schema);

        System.out.println("===== Input Data =====");
        input.show(false); // false allow us to remove truncation for long strings and other datatype . if kept default true it will look like ... in every field

        /*
         * WINDOW FUNCTION APPROACH
         * ------------------------
         *
         * Requirement:
         * Keep all original rows and add a count column.
         *
         * If we use only groupBy():
         *
         * input.groupBy(...)
         *      .count();
         *
         * Spark will aggregate records and return only grouped rows.
         * Original rows are lost.
         *
         * Therefore, Window Function is a better choice because:
         *
         * 1. It performs aggregation.
         * 2. Preserves original rows.
         * 3. Adds aggregate results back to each row.
         */

        /*
         * Create a Window Specification.
         *
         * partitionBy() is similar to SQL:
         *
         * PARTITION BY column1, column2, label
         *
         * Spark logically divides the data into groups.
         *
         * Example:
         *
         * Partition 1:
         * (IP1.54880, IP2.5001, 2)
         *
         * Partition 2:
         * (IP2.5001, IP1.54880, 2)
         *
         * Count calculation happens independently inside each partition.
         */
        WindowSpec partitionWindow = Window.partitionBy(
                "column1",
                "column2",
                "label"
        );

        /*
         * Add a new column named "count".
         *
         * count(lit(1))
         * -------------
         * Counts every row in the partition.
         *
         * over(partitionWindow)
         * ---------------------
         * Executes the count calculation inside each partition.
         *
         * Example:
         *
         * If a partition contains 5 records:
         *
         * row1 -> count = 5
         * row2 -> count = 5
         * row3 -> count = 5
         * row4 -> count = 5
         * row5 -> count = 5
         *
         * Important:
         * Unlike groupBy(), rows are NOT collapsed.
         */
        Dataset<Row> solution = input.withColumn(
                "count",
                count(lit(1)).over(partitionWindow)
        );

        /*
        count(lit(1)).over(partitionWindow) is a window aggregation. lit(1) creates a constant column and count() counts the
         number of rows within each window partition. The over(partitionWindow) clause tells Spark to perform the count separately
         for every (column1, column2, label) group while preserving all original rows. This allows us to enrich each row with the
         group's count without using a separate groupBy and join operation.
         */

        System.out.println("===== Result =====");
        solution.show(false);

        /*
         * Gracefully stop Spark and release resources.
         */
        spark.stop();
    }
}