
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day21SparkCatalog {

  def main(args: Array[String]): Unit = {

    // 1. Create Spark session

val spark = SparkSession.builder()
  .appName("Day21-Spark-Catalog")
  .master("local[*]")
  .config("spark.driver.host", "127.0.0.1")
  .config("spark.driver.bindAddress", "127.0.0.1")
  .config("spark.sql.warehouse.dir", "file:///tmp/day21-spark-warehouse")
  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    println("\n========== DAY 21: SPARK CATALOG ==========")

    // 2. Create hotel booking data
    val bookings = Seq(
      (101, "Prabhat", "Taj Palace", "Delhi", 2, 12000.0, "Confirmed"),
      (102, "Rahul", "Grand Hyatt", "Mumbai", 3, 18000.0, "Confirmed"),
      (103, "Amit", "Taj Palace", "Delhi", 1, 6000.0, "Cancelled"),
      (104, "Sneha", "Oberoi", "Bengaluru", 4, 24000.0, "Confirmed"),
      (105, "Priya", "Grand Hyatt", "Mumbai", 2, 14000.0, "Pending"),
      (106, "Rohit", "Oberoi", "Bengaluru", 3, 21000.0, "Confirmed"),
      (107, "Neha", "Taj Palace", "Delhi", 2, 11000.0, "Confirmed"),
      (108, "Vikas", "ITC Hotel", "Hyderabad", 1, 5000.0, "Cancelled"),
      (109, "Anjali", "ITC Hotel", "Hyderabad", 3, 15000.0, "Confirmed"),
      (110, "Karan", "Grand Hyatt", "Mumbai", 1, 8000.0, "Pending"),
      (111, "Pooja", "Oberoi", "Bengaluru", 2, 16000.0, "Confirmed"),
      (112, "Arjun", "ITC Hotel", "Hyderabad", 2, 10000.0, "Confirmed")
    ).toDF(
      "booking_id",
      "customer_name",
      "hotel_name",
      "city",
      "nights",
      "total_amount",
      "booking_status"
    )

    println("\n--- 1. Original Booking Data ---")
    bookings.show(false)

    // 3. Create a database
    spark.sql("CREATE DATABASE IF NOT EXISTS hotel_analytics")
    spark.sql("USE hotel_analytics")

    println("\n--- 2. List Databases ---")
    spark.catalog.listDatabases().show(false)

    // 4. Create a temporary view
    bookings.createOrReplaceTempView("bookings_view")

    println("\n--- 3. Temporary View Query ---")
    spark.sql("""
      SELECT city, COUNT(*) AS total_bookings
      FROM bookings_view
      GROUP BY city
      ORDER BY total_bookings DESC
    """).show(false)

    // 5. Register a persistent managed table
    bookings.write
      .mode("overwrite")
      .saveAsTable("hotel_bookings")

    println("\n--- 4. List Tables in Current Database ---")
    spark.catalog.listTables().show(false)

    // 6. Query the registered table
    println("\n--- 5. Confirmed Bookings ---")
    spark.sql("""
      SELECT booking_id, customer_name, hotel_name, total_amount
      FROM hotel_bookings
      WHERE booking_status = 'Confirmed'
      ORDER BY total_amount DESC
    """).show(false)

    // 7. Inspect the schema
    println("\n--- 6. DataFrame Schema ---")
    bookings.printSchema()

    println("\n--- 7. SQL Table Description ---")
    spark.sql("DESCRIBE hotel_bookings").show(false)

    // 8. Inspect columns and table metadata
    println("\n--- 8. Catalog Columns ---")
    spark.catalog.listColumns("hotel_bookings").show(false)

    println("\n--- 9. Table Exists? ---")
    println(spark.catalog.tableExists("hotel_bookings"))

    // 9. Hotel revenue analysis
    println("\n--- 10. Confirmed Revenue by Hotel ---")
    spark.sql("""
      SELECT hotel_name,
             COUNT(*) AS confirmed_bookings,
             SUM(total_amount) AS confirmed_revenue
      FROM hotel_bookings
      WHERE booking_status = 'Confirmed'
      GROUP BY hotel_name
      ORDER BY confirmed_revenue DESC
    """).show(false)

    println("\n========== PROJECT COMPLETED ==========")

    spark.stop()
  }
}
