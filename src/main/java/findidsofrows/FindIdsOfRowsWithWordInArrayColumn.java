package findidsofrows;

import java.util.Arrays;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.*;

public class FindIdsOfRowsWithWordInArrayColumn {

    public static void main(String[] args) {
        //Find all IDs where a word exists in the words column.
        SparkSession spark = SparkSession.builder()
                .appName("Exercise")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create(1, "one,two,three", "one"),
                        RowFactory.create(2, "four,one,five", "six"),
                        RowFactory.create(3, "seven,nine,one,two", "eight"),
                        RowFactory.create(4, "two,three,five", "five"),
                        RowFactory.create(5, "six,five,one", "seven")
                ),
                new StructType()
                        .add("id", DataTypes.IntegerType)
                        .add("words", DataTypes.StringType)
                        .add("word", DataTypes.StringType)
        );

        Dataset<Row> explodedWords = df.select(
                col("id"),
                explode(split(col("words"), ",")).alias("w")
        );
        System.out.println("explode words -----");
        explodedWords.show(false);

        Dataset<Row> lookupWords = df
                .select(col("word").alias("w"))
                .distinct();

        System.out.println("lookupwords-----");
        lookupWords.show(false);



        Dataset<Row> result = explodedWords
                .join(lookupWords, "w")
                .groupBy("w")
                .agg(
                        sort_array(
                                collect_list(col("id"))
                        ).alias("ids")
                )
                .orderBy("w");
        /*
        The join filters only the words that exist in the word column. groupBy("w") and
        collect_list(id) aggregate all matching row IDs for each word, while sort_array()
         ensures the IDs appear in ascending order.
         */
        result.show(false);

        spark.stop();
    }
}
