package mergetworows;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.first;

public class MergeTwoRows {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[*]")
                .appName("merge two rows")
                .getOrCreate();

        List<Row> rows = Arrays.asList(
                RowFactory.create("100", "John", 35, null),
                RowFactory.create("100", "John", null, "Georgia"),
                RowFactory.create("101", "Mike", 25, null),
                RowFactory.create("101", "Mike", null, "New York"),
                RowFactory.create("103", "Mary", 22, null),
                RowFactory.create("103", "Mary", null, "Texas"),
                RowFactory.create("104", "Smith", 25, null),
                RowFactory.create("105", "Jake", null, "Florida")
        );

        StructType schema = new StructType()
                .add("id","String")
                .add("name","String")
                .add("age","integer")
                .add("city","String");

        Dataset<Row> df1 = spark.createDataFrame(rows,schema);
        df1.show();

        Dataset<Row> df2 = df1.groupBy("id").agg(first("name", true).alias("name"),
                             first("age", true).alias("age"),
                        first("city", true).alias("city"));

        df2.show();

        /*
        The records are split across multiple rows for the same id, where different columns contain null values.
        To merge them, I group by id and use first(column, true) on every non-key column.
        The second parameter true tells Spark to ignore null values and return the first available value.
        This allows Spark to consolidate multiple partial records into a single complete record without using joins or window functions.
         */
    }
}
