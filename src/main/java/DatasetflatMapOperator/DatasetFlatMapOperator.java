package DatasetflatMapOperator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

public class DatasetFlatMapOperator {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Dataset FlatMap Operator")
                .getOrCreate();

        List<Row> rows = List.of(
                RowFactory.create((Object) new Integer[]{1, 2, 3})
        );

        StructType structType = new StructType()
                .add("nums", DataTypes.createArrayType(DataTypes.IntegerType));

        Dataset<Row> df1 = spark.createDataFrame(rows,structType);
        df1.show(false);

        Dataset<Row> df2 = df1.withColumn("num", explode(col("nums")));
        df2.show();

        // what explode is doing here "Take one row containing an array and create one new row for each element of that array."

        //explode() converts each element of an array into a separate row, and when used with withColumn,
        // Spark duplicates the original row for every array element while placing the current element in the new column.

        //Expected way of solution
        /*
        StructType outputSchema = new StructType()
                .add("nums", DataTypes.createArrayType(DataTypes.IntegerType))
                .add("num", DataTypes.IntegerType);

        Dataset<Row> result = df1.flatMap(
                (FlatMapFunction<Row, Row>) row -> {

                    List<Integer> nums = row.getList(0);

                    List<Row> output = new ArrayList<>();

                    for (Integer num : nums) {
                        output.add(
                                RowFactory.create(nums, num)
                        );
                    }

                    return output.iterator();
                },
                RowEncoder.encoderFor(outputSchema)
        );

        result.show(false);

         */
        spark.stop();

    }
}
