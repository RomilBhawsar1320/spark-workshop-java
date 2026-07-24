package splitFunction;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;

public class SplitFunction {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("SplitFunction")
                .master("local[*]")
                .getOrCreate();


        List<Row> data = Arrays.asList(
                RowFactory.create("50000.0#0#0#", "#"),
                RowFactory.create("0@1000.0@", "@"),
                RowFactory.create("1$", "$"),
                RowFactory.create("1000.00^Test_string", "^")
        );

        StructType schema = new StructType()
                .add("VALUES", DataTypes.StringType)
                .add("Delimiter", DataTypes.StringType);

        Dataset<Row> dept = spark.createDataFrame(data, schema);
        dept.show(false);

        Dataset<Row> solution1 = dept.select(col("VALUES"),col("Delimiter"),
                expr("split(VALUES, Delimiter)").alias("split_values"));

        solution1.show();

        dept.createOrReplaceTempView("dept");

        Dataset<Row> solution = spark.sql("""
        SELECT
        VALUES,
        Delimiter,
        split(VALUES, Delimiter) AS split_values
         FROM dept
        """);

        solution.show(false);

    }


}
