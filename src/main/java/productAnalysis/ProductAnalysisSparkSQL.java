package productAnalysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ProductAnalysisSparkSQL {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Product Analysis Using Spark SQL")
                .master("local[*]")
                .getOrCreate();

        // Read CSV
        Dataset<Row> products = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/products.csv");

        // Create Temp View
        products.createOrReplaceTempView("products");

        // Question 1
        System.out.println("Total Products");
        spark.sql(
                "SELECT COUNT(*) AS total_products FROM products"
        ).show();

        // Question 2
        System.out.println("Products having stock less than 75");
        spark.sql(
                "SELECT * " +
                        "FROM products " +
                        "WHERE stock_quantity < 75"
        ).show();

        // Question 3
        System.out.println("Electronics products costing more than 150");
        spark.sql(
                "SELECT * " +
                        "FROM products " +
                        "WHERE category='Electronics' " +
                        "AND price > 150"
        ).show();

        // Question 4
        System.out.println("Inventory Value");

        spark.sql(
                "SELECT " +
                        "category, " +
                        "SUM(price * stock_quantity) AS total_inventory_value " +
                        "FROM products " +
                        "GROUP BY category"
        ).show();

        // Question 5
        System.out.println("Average Min Max Price");

        spark.sql(
                "SELECT " +
                        "category, " +
                        "AVG(price) AS average_price, " +
                        "MIN(price) AS minimum_price, " +
                        "MAX(price) AS maximum_price " +
                        "FROM products " +
                        "GROUP BY category"
        ).show();

        // Question 6
        System.out.println("Top 3 Expensive Products");

        spark.sql(
                "WITH ranked_products AS ( " +
                        "SELECT *, " +
                        "ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rank " +
                        "FROM products " +
                        ") " +
                        "SELECT * " +
                        "FROM ranked_products " +
                        "WHERE rank <= 3"
        ).show(false);

        // Question 7
        System.out.println("Old Price vs New Price");

        spark.sql(
                "WITH updated_price AS ( " +
                        "SELECT " +
                        "product_id, " +
                        "product_name, " +
                        "price, " +
                        "price * (RAND() * 0.9 + 1.1) AS new_price " +
                        "FROM products " +
                        ") " +
                        "SELECT " +
                        "old.product_id, " +
                        "old.product_name, " +
                        "old.price AS old_price, " +
                        "ROUND(new.new_price, 2) AS new_price, " +
                        "ROUND(new.new_price - old.price, 2) AS difference " +
                        "FROM products old " +
                        "JOIN updated_price new " +
                        "ON old.product_id = new.product_id"
        ).show(false);

        spark.stop();
    }
}