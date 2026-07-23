package productAnalysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class ProductanalysisApplication {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Product Analysis")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> products = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/products.csv")
                .cache();

        products.show();

        // Question 1

        System.out.println("Total Products : " + products.count());

        // Question 2

        System.out.println("Products having stock less than 75");

        products.filter(col("stock_quantity").lt(75))
                .show();


//        Question 3

        System.out.println("Electronics products costing more than 150");

        products.filter(
                        col("category").equalTo("Electronics")
                                .and(col("price").gt(150))
                )
                .show();

//      Question 4

        System.out.println("Inventory Value");
        Dataset<Row> inventory = products
                .withColumn("inventory_value",
                        col("price").multiply(col("stock_quantity")));

        inventory.groupBy("category")
                .agg(sum("inventory_value").alias("Total Inventory Value"))
                .show();

//      Question 5

        System.out.println("Average Min Max Price");
        products.groupBy("category")
                .agg(
                        avg("price").alias("Average Price"),
                        min("price").alias("Minimum Price"),
                        max("price").alias("Maximum Price")
                ).show();

        //      Question 6

        WindowSpec window = Window.partitionBy("category")
                .orderBy(col("price").desc());
        Dataset<Row> top3 = products
                .withColumn("rank",
                        row_number().over(window))
                .filter(col("rank").leq(3));
        System.out.println("Top 3 expensive products");
        top3.show();

//    Question 7

        Dataset<Row> updatedPrice = products
                .withColumn(
                        "new_price",
                        col("price")
                                .multiply(rand().multiply(0.9).plus(1.1))
                );



        Dataset<Row> result = products.alias("old")
                .join(
                        updatedPrice.alias("new"),
                        col("old.product_id")
                                .equalTo(col("new.product_id"))
                )
                .select(
                        col("old.product_id"),
                        col("old.product_name"),
                        col("old.price").alias("Old Price"),
                        round(col("new.new_price"),2)
                                .alias("New Price"),
                        round(
                                col("new.new_price")
                                        .minus(col("old.price")),
                                2
                        ).alias("Difference")
                );

        result.show(false);

        spark.stop();
    }
}

