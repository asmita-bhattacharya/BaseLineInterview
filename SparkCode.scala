package org.example
import org.apache.spark.{SparkConf,SparkContext}

object SparkCode {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Testing Intellij").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark Program")
      .getOrCreate
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val usa_data="C:\\Users\\LENOVO\\OneDrive\\Documents\\learners'Docs\\usa_data.csv"
    val DataFrame1=spark.read.format("csv").option("header", "true").load(usa_data)
    val df_usa = DataFrame1.select(
      DataFrame1.col("year").cast("string"),
      DataFrame1.col("barley_harvest").cast("integer"),
      DataFrame1.col("beef_slaughter").cast("integer"),
      DataFrame1.col("corn_harvest").cast("integer"),
      DataFrame1.col("cotton_harvest").cast("integer"),
      DataFrame1.col("pork_slaughter").cast("integer"),
      DataFrame1.col("poultry").cast("integer"),
      DataFrame1.col("rice_harvest").cast("integer"),
      DataFrame1.col("sorghum_harvest").cast("integer"),
      DataFrame1.col("soybeans_harvest").cast("integer"),
      DataFrame1.col("soybean_meal").cast("integer"),
      DataFrame1.col("soybean_oil").cast("integer"),
      DataFrame1.col("wheat_harvest").cast("integer")
    )
      .withColumnRenamed("barley_harvest","usa_barley_harvest")
      .withColumnRenamed("beef_slaughter","usa_beef_slaughter")
      .withColumnRenamed("corn_harvest","usa_corn_harvest")
      .withColumnRenamed("cotton_harvest","usa_cotton_harvest")
      .withColumnRenamed("pork_slaughter","usa_pork_slaughter")
      .withColumnRenamed("poultry","usa_poultry")
      .withColumnRenamed("rice_harvest","usa_rice_harvest")
      .withColumnRenamed("sorghum_harvest","usa_sorghum_harvest")
      .withColumnRenamed("soybeans_harvest","usa_soybeans_harvest")
      .withColumnRenamed("soybean_meal","usa_soybean_meal")
      .withColumnRenamed("soybean_oil","usa_soybean_oil")
      .withColumnRenamed("wheat_harvest","usa_wheat_harvest")


    val world_data="C:\\Users\\LENOVO\\OneDrive\\Documents\\learners'Docs\\world_data.csv"
    val DataFrame2=spark.read.format("csv").option("header", "true").load(world_data)
    val df_world = DataFrame2.select(
      DataFrame2.col("year").cast("string"),
      DataFrame2.col("barley_harvest").cast("integer"),
      DataFrame2.col("beef_slaughter").cast("integer"),
      DataFrame2.col("corn_harvest").cast("integer"),
      DataFrame2.col("cotton_harvest").cast("integer"),
      DataFrame2.col("pork_slaughter").cast("integer"),
      DataFrame2.col("poultry").cast("integer"),
      DataFrame2.col("rice_harvest").cast("integer"),
      DataFrame2.col("sorghum_harvest").cast("integer"),
      DataFrame2.col("soybeans_harvest").cast("integer"),
      DataFrame2.col("soybean_meal").cast("integer"),
      DataFrame2.col("soybean_oil").cast("integer"),
      DataFrame2.col("wheat_harvest").cast("integer")
    )
      .withColumnRenamed("year","crop_year")
      .withColumnRenamed("barley_harvest","world_barley_harvest")
      .withColumnRenamed("beef_slaughter","world_beef_slaughter")
      .withColumnRenamed("corn_harvest","world_corn_harvest")
      .withColumnRenamed("cotton_harvest","world_cotton_harvest")
      .withColumnRenamed("pork_slaughter","world_pork_slaughter")
      .withColumnRenamed("poultry","world_poultry")
      .withColumnRenamed("rice_harvest","world_rice_harvest")
      .withColumnRenamed("sorghum_harvest","world_sorghum_harvest")
      .withColumnRenamed("soybeans_harvest","world_soybeans_harvest")
      .withColumnRenamed("soybean_meal","world_soybean_meal")
      .withColumnRenamed("soybean_oil","world_soybean_oil")
      .withColumnRenamed("wheat_harvest","world_wheat_harvest")

    val finalReport=df_usa.join(df_world, df_usa("year") === df_world("crop_year"), "inner")
      .withColumn("usa_barley_contribution", col("usa_barley_harvest") / col("world_barley_harvest") * 100)
      .withColumn("usa_beef_contribution", col("usa_beef_slaughter") / col("world_beef_slaughter") * 100)
      .withColumn("usa_corn_contribution", col("usa_corn_harvest") / col("world_corn_harvest") * 100)
      .withColumn("usa_cotton_contribution", col("usa_cotton_harvest") / col("world_cotton_harvest") * 100)
      .withColumn("usa_pork_contribution", col("usa_pork_slaughter") / col("world_pork_slaughter") * 100)
      .withColumn("usa_poultry_contribution", col("usa_poultry") / col("world_poultry") * 100)
      .withColumn("usa_rice_contribution", col("usa_rice_harvest") / col("world_rice_harvest") * 100)
      .withColumn("usa_sorghum_contribution", col("usa_sorghum_harvest") / col("world_sorghum_harvest") * 100)
      .withColumn("usa_soybeans_contribution", col("usa_soybeans_harvest") / col("world_soybeans_harvest") * 100)
      .withColumn("usa_soybean_meal_contribution", col("usa_soybean_meal") / col("world_soybean_meal") * 100)
      .withColumn("usa_soybean_oil_contribution", col("usa_soybean_oil") / col("world_soybean_oil") * 100)
      .withColumn("usa_wheat_contribution", col("usa_wheat_harvest") / col("world_wheat_harvest") * 100)
      .select($"crop_year", $"world_barley_harvest", $"usa_barley_contribution",$"world_beef_slaughter", $"usa_beef_contribution",
        $"world_corn_harvest", $"usa_corn_contribution",$"world_cotton_harvest", $"usa_cotton_contribution",
        $"world_pork_slaughter", $"usa_pork_contribution",$"world_wheat_harvest", $"usa_wheat_contribution",
        $"world_poultry", $"usa_poultry_contribution",$"world_soybean_meal", $"usa_soybean_meal_contribution",
        $"world_rice_harvest", $"usa_rice_contribution",$"world_soybeans_harvest", $"usa_soybeans_contribution",
        $"world_sorghum_harvest", $"usa_sorghum_contribution",$"world_soybean_oil", $"usa_soybean_oil_contribution")

    finalReport.show()
  }
}
