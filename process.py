from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, regexp_replace, count, max, min, round

spark = SparkSession.builder.appName("data-assessment").getOrCreate()

#read csvs in
devices = spark.read.csv("./devices.csv", header=True, inferSchema=True)
events = spark.read.csv("./events.csv", header=True, inferSchema=True)

#join csvs together to enrich data
joined = events.join(devices, events.deviceId == devices.ID, "inner")

#count app_opened_events
app_opened_count = joined.filter(joined['type'] == "APP_OPENED").count()
print("Number of times app was opened: ", app_opened_count)

#cast to float and multiply by 100 to work with ints instead of floating point
cleaned_price = joined.withColumn("totalPrice", regexp_replace(col("totalPrice"), "\\$", "").cast("double"))
cleaned_price = cleaned_price.withColumn("totalPrice",round(col("totalPrice") * 100).cast("int"))

#sum total price and divide by 100 to get amount in dollars and cents
total_price_add_to_cart = cleaned_price.filter(joined['type'] == "ADD_TO_CART").agg(sum('totalPrice')).collect()[0][0]
total_price_add_to_cart = f"${total_price_add_to_cart/100}"
print("Total price of items added to cart: ", total_price_add_to_cart)

#for each session calculate the max and min timestamp, find the difference and take the average among all sessions
session_counts = cleaned_price.groupBy("session").agg(
    count("*").alias("event_count"),
    max("timestamp").alias("latest_timestamp"),
    min("timestamp").alias("earliest_timestamp")
).filter(col("event_count") >= 2)

session_lengths = session_counts.withColumn(
    "session_length",
    col("latest_timestamp") - col("earliest_timestamp")
)

average_session_length = session_lengths.agg(
    {"session_length": "avg"}
).collect()[0][0]

print("Average session length in seconds: ", average_session_length)

#write the csvs
calculation_results = [(app_opened_count, total_price_add_to_cart, average_session_length)]
columns = ["app_opened_count", "total_price_add_to_cart", "average_session_length"]
calculation_results_df = spark.createDataFrame(calculation_results, columns)

output_path = "./calculation_results"
calculation_results_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")


output_path = "./enriched_data"
enriched_data_selection = joined.select("session", "deviceId", "timestamp", "timstamp_iso", "type", "totalPrice", "User")
enriched_data_selection.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

#join total price per session with session length
sum_price_per_session = cleaned_price.groupBy("session").agg(sum("totalPrice").alias("sum_price_per_session"))
sum_price_per_session = sum_price_per_session.withColumn("sum_price_per_session", round(col("sum_price_per_session")/100, 2))

session_lengths = session_lengths.join(sum_price_per_session, "session", "inner")
selection = session_lengths.select("session", "session_length", "sum_price_per_session")

output_path = "./bonus"
selection.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

spark.stop()
