package importantrows;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SelectMostImportantRows {
    public static void main(String[] args) {

        //step 1 create spark session
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("select most important row")
                .getOrCreate();
        // step 2 create input collection
        List<Row> rows = Arrays.asList(
                RowFactory.create(1,"MV1"),
                RowFactory.create(1,"MV2"),
                RowFactory.create(2,"VPV"),
                RowFactory.create(2,"Others")

        );
        // step 3 create schema using StructType
        StructType schema = new StructType()
                .add("ID","integer")
                .add("value","String");
        // step 4 create input dataframe
        Dataset<Row> input = spark.createDataFrame(rows,schema);
        System.out.println("Input Data");
        input.show(false);

        // Assign business priority. Lower number means higher priority.MV1 -> 1, MV2 -> 2 etc.
        Dataset<Row> withPriority = input.withColumn("priority",
                when(col("value").equalTo("MV1"),1).
                when(col("value").equalTo("VPV"),1).
                when(col("value").equalTo("MV2"),2).otherwise(99));

        withPriority.show(false);

        //Create a window per id. Within every id, sort records by priority.
        //A window function allows Spark to perform calculations across a group of rows without collapsing them into a single row.
        WindowSpec windowSpec = Window.partitionBy("id").orderBy("priority");

        // creating a ranked DataFrame
        Dataset<Row> rankedDataFrame = withPriority.withColumn("rank",row_number().over(windowSpec));
        rankedDataFrame.show(false);

        //final solution
        Dataset<Row> solution = rankedDataFrame.filter(col("rank").equalTo(1))
                .select("id","value");
        System.out.println("Final Solution");
        solution.show(false);
        spark.stop();
    }
}
