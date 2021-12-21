import java.util.Random;
import static java.lang.Math.*;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import static org.apache.spark.sql.functions.*;

/**
* Class TaxiDataGenerator
*
* Performs mock taxi data generation task based on given datasets
*/
public class TaxiDataGenerator {

  /**
  * Static function generateNumber
  *
  * Generate a random double value in a given range (min, max)
  */
  public static double generateNumber(int min, int max) {
    return Math.random() * (max - min) + min;
  }

  /**
  * Main function
  */
  public static void main(String[] args) {
    String logFile = "/data/logs/README.md"; // Should be some file on your system
    SparkSession spark = SparkSession.builder().appName("Taxi Data Generator").getOrCreate();

    /**
    * UDF (User-Defined Function)
    *
    * Generates random *cost* value for each of the entities in range **3...20**
    * euro - a median cost of taxi cab in London.
    */
    UserDefinedFunction generateCostValue = udf(
      () -> generateNumber(6, 100), DataTypes.FloatType
    );
    generateRatingValue.asNondeterministic();
    spark.udf().register("generateCostValue", generateCostValue);

    /**
    * Read csv data into spark dataframe
    *
    * Drops unusable columns, standardizes column names and caches data
    *   .select() - selects specified columns from a dataset
    *   .withColumnRenamed() - renames a given column by a given name
    *   .cache() - pulls datasets into a cluster-wide in-memory cache
    */
    Dataset<Row> london_postcodes_pickup = spark.read().csv("/data/london_postcodes.csv")
      .select("Postcode", "Latitude", "Longitude")
      .withColumnRenamed("Postcode", "postcode")
      .withColumnRenamed("Latitude", "latitude")
      .withColumnRenamed("Longitude", "longitude")
      .cache();

    Dataset<Row> london_postcodes_dropoff = spark.read().csv("/data/london_postcodes.csv")
      .select("Postcode", "Latitude", "Longitude")
      .withColumnRenamed("Postcode", "postcode")
      .withColumnRenamed("Latitude", "latitude")
      .withColumnRenamed("Longitude", "longitude")
      .cache();

    Dataset<Row> taxi_durations = spark.read().csv("/data/taxi_durations.csv")
      .select("pickup_datetime", "dropoff_datetime")
      .withColumnRenamed("pickup_datetime", "pickupDateTime")
      .withColumnRenamed("dropoff_datetime", "dropoffDateTime")
      .cache();

    Dataset<Row> driver_reviews = spark.read().csv("/data/driver_reviews.csv")
      .select("userName", "content", "score")
      .withColumnRenamed("userName", "driverName")
      .withColumnRenamed("content", "driverComment")
      .withColumnRenamed("score", "driverRating")
      .cache();

    Dataset<Row> customer_reviews = spark.read().csv("/data/customer_reviews.csv")
      .select("reviews.username", "reviews.title")
      .withColumnRenamed("reviews.username", "clientName")
      .withColumnRenamed("reviews.title", "clientComment")
      .withColumnRenamed("reviews.text", "clientRating")
      .cache();
    /** */


    /**
    * Data Multiplication
    *
    * Multiplies a number of rows in the datasets by a given factor.
    * For each dataset:
    *   1.   Calculate the correlation factor in respect to the main dataset
    *   2.   If correlation_factor < 1 : multiply dataset by **correlation_factor * global_factor**
    *
    * The point is to align number of rows of each dataset in respect to the main
    * dataset - it allows to generate a full dataset with minimal number of NaN values.
    */
    int factor = 100;
    int pickup_row_count = london_postcodes_pickup.count();
    double dropoff_factor = pickup_row_count / london_postcodes_dropoff.count();
    double durations_factor = pickup_row_count / taxi_durations.count();
    double driver_reviews_factor = pickup_row_count / driver_reviews.count();
    double customer_reviews_factor = pickup_row_count / customer_reviews.count();

    london_postcodes_pickup.withColumn("a", expr(f"explode(array_repeat(0,{factor}))")).drop("a")

    if (dropoff_factor < 1) {
      london_postcodes_dropoff.withColumn("a", expr(f"explode(array_repeat(0,{int(dropoff_factor) * factor}))")).drop("a");
    }
    if (durations_factor < 1) {
      taxi_durations.withColumn("a", expr(f"explode(array_repeat(0,{int(durations_factor) * factor}))")).drop("a");
    }
    if (driver_reviews_factor < 1) {
      driver_reviews.withColumn("a", expr(f"explode(array_repeat(0,{int(driver_reviews_factor) * factor}))")).drop("a");
    }
    if (customer_reviews_factor < 1) {
      customer_reviews.withColumn("a", expr(f"explode(array_repeat(0,{int(customer_reviews_factor) * factor}))")).drop("a");
    }
    /** */

    /**
    * Data Shuffling and Identity Provisioning
    *
    * Shuffle entities in the datasets. Assign an unique **id** for each entity of the dataset.
    * Operation allows to generate a huge dataset with minimal number of identical entities.
    */
    london_postcodes_pickup.orderBy(rand()).withColumn("id", monotonically_increasing_id());
    london_postcodes_dropoff.orderBy(rand()).withColumn("id", monotonically_increasing_id());
    taxi_durations.orderBy(rand()).withColumn("id", monotonically_increasing_id());
    driver_reviews.orderBy(rand()).withColumn("id", monotonically_increasing_id());
    customer_reviews.orderBy(rand()).withColumn("id", monotonically_increasing_id());
    /** */

    /**
    * Cost Data Generation
    *
    * Generate random *cost* value for each of the entities in range **3...20** euro
    * - a median cost of taxi cab in London.
    */
    london_postcodes_pickup.withColumn("cost", callUDF("generateCostValue"));


    /**
    * Join operation on multiple datasets
    *
    * Perform *LEFT join* operation on multiple datasets.
    * Number of entities in each of the datasets is approximately equal, so there is no big data loss.
    * Drop *id* column to avoid *mutiple identical columns* exceptions -
    * there is no need in the column anymore due to complete join operation.
    */
    london_postcodes_pickup.join(london_postcodes_dropoff, london_postcodes_pickup.col("id").equalTo(london_postcodes_dropoff.col("id")), "left")
      .join(taxi_durations, london_postcodes_pickup.col("id").equalTo(taxi_durations.col("id")), "left")
      .join(driver_reviews, london_postcodes_pickup.col("id").equalTo(driver_reviews.col("id")), "left")
      .join(customer_reviews, london_postcodes_pickup.col("id").equalTo(customer_reviews.col("id")), "left")
      .show(false);

    // store generated taxi_data in csv format
    london_postcodes_pickup.write()
      .option("header", "true")
      .csv("/data/taxi_data");

    /**
    * Group Task: What is the main cause of negative comments?
    *
    * To answer that question we've implemented an algorithm that perform next operations:
    *   1.   Select negative comments by filtering comments with low rating value
    *   2.   Count number of words that is present in each of the comment
    *   3.   Drop low-informative words (pronouns, articles, etc)
    *
    * As a result, we see the most often words that is present in negative comments section.
    */
    Dataset<Row> bad_ratings_data = london_postcodes_pickup
      .select("clientComment")
      .filter("clientComment < 3");

    bad_ratings_data.withColumn("word", explode(split(col("clientComment"), " ")))
      .groupBy("word")
      .count()
      .sort("count")
      .where(bad_ratings_data.word != "he")
      .where(bad_ratings_data.col("word") != "she")
      .where(bad_ratings_data.col("word") != "it")
      .where(bad_ratings_data.col("word") != "I")
      .where(bad_ratings_data.col("word") != "the")
      .where(bad_ratings_data.col("word") != "a")
      .where(bad_ratings_data.col("word") != "this")
      .where(bad_ratings_data.col("word") != "that");
    /** */

    // store bad_rating_reviews data in csv format
    bad_rating_reviews.write()
      .option("header", "true")
      .csv("/data/bad_ratings_data");

    spark.stop();
  }
}
