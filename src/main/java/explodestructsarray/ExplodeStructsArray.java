package explodestructsarray;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.explode;

public class ExplodeStructsArray {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Explode Structs Array")
                .getOrCreate();

//        StructType daySchema = new StructType()
//                .add("close","String")
//                .add("open","String");
//
//        StructType hourSchema = new StructType()
//                .add("Monday",DataTypes.createArrayType(daySchema))
//                .add("Tuesday",DataTypes.createArrayType(daySchema))
//                .add("Wednesday",DataTypes.createArrayType(daySchema))
//                .add("Thursday",DataTypes.createArrayType(daySchema))
//                .add("Friday",DataTypes.createArrayType(daySchema))
//                .add("Saturday",DataTypes.createArrayType(daySchema))
//                .add("Sunday",DataTypes.createArrayType(daySchema));
//
//
//        StructType schema = new StructType()
//                .add("business_id","String")
//                .add("full_address","String")
//                .add("hours", DataTypes.createArrayType(hourSchema));

        StructType daySchema = new StructType()
                .add("close", "string")
                .add("open", "string");

        StructType hourSchema = new StructType()
                .add("Monday", daySchema)
                .add("Tuesday", daySchema)
                .add("Wednesday", daySchema)
                .add("Thursday", daySchema)
                .add("Friday", daySchema)
                .add("Saturday", daySchema)
                .add("Sunday", daySchema);

        StructType schema = new StructType()
                .add("business_id", "string")
                .add("full_address", "string")
                .add("hours", hourSchema);

        Dataset<Row> df1 = spark.read()
                .option("multiline", true)
                .schema(schema)
                .json("src/main/resources/input.json");

        df1.show(false);
        df1.printSchema();

        Dataset<Row> solution = df1.selectExpr(
                "business_id",
                "full_address",
                "stack(7, " +
                        "'Monday', hours.Monday.open, hours.Monday.close, " +
                        "'Tuesday', hours.Tuesday.open, hours.Tuesday.close, " +
                        "'Wednesday', hours.Wednesday.open, hours.Wednesday.close, " +
                        "'Thursday', hours.Thursday.open, hours.Thursday.close, " +
                        "'Friday', hours.Friday.open, hours.Friday.close, " +
                        "'Saturday', hours.Saturday.open, hours.Saturday.close, " +
                        "'Sunday', hours.Sunday.open, hours.Sunday.close" +
                        ") as (day, open_time, close_time)"
        );

        solution.show(false);

        /*
        Dataset<Row> solution = df1
    .select(
        col("business_id"),
        col("full_address"),
        explode(array(
                struct(lit("Monday").alias("day"), col("hours.Monday").alias("info")),
                struct(lit("Tuesday").alias("day"), col("hours.Tuesday").alias("info")),
                struct(lit("Wednesday").alias("day"), col("hours.Wednesday").alias("info")),
                struct(lit("Thursday").alias("day"), col("hours.Thursday").alias("info")),
                struct(lit("Friday").alias("day"), col("hours.Friday").alias("info")),
                struct(lit("Saturday").alias("day"), col("hours.Saturday").alias("info")),
                struct(lit("Sunday").alias("day"), col("hours.Sunday").alias("info"))
        )).alias("day_info")
    )
    .select(
        col("business_id"),
        col("full_address"),
        col("day_info.day").alias("day"),
        col("day_info.info.open").alias("open_time"),
        col("day_info.info.close").alias("close_time")
    );

solution.show(false);
         */

    }
}
