package FlatteningArrayColumns;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.spark.sql.functions.*;

public class FlatteningArrayColumns {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Flattening Array Column")
                .getOrCreate();

        StructType schema = new StructType()
                .add("value", DataTypes.createArrayType(DataTypes.StringType));

        List<Row> rows = Arrays.asList(
                RowFactory.create((Object) new String[]{"a","b","c"}),
                RowFactory.create((Object) new String[]{"X","Y","Z"})
        );

        Dataset<Row> df1 = spark.createDataFrame(rows,schema);
//        df1.show(false);
        df1.printSchema();

        Dataset<Row> explodeEachElement = df1.select(posexplode(col("value")));
//        explodeEachElement.show(false);

        Dataset<Row> pivotexplode = explodeEachElement.groupBy("pos").pivot("pos")
                .agg(functions.first("col"));
        pivotexplode.show(false);





//        Dataset<Row> exploded = df1
//                .withColumn("id", functions.monotonically_increasing_id())
//                .select(
//                        col("id"),
//                        functions.posexplode(col("value"))
//                );
//
//        exploded.show(false);

        Dataset<Row> solution = df1.select(
                col("value").getItem(0).alias("0"),
                col("value").getItem(1).alias("1"),
                col("value").getItem(2).alias("2")
        );

        solution.show(false); //(assuming all arrays have the same length)




    }


}
