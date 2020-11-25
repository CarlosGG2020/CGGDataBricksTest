// Databricks notebook source
/*val containerName = "azsqlshackcontainer"
val storageAccountName = "azsqlshackstorage"
val sas = "?sv=2019-02-02&ss=b&srt=sco&sp=rwdlac&se=2020-03-30T07:53:28Z&st=2020-03-29T23:53:28Z&spr=https&sig=5vK%2FKEgTJVLoF4SX08IvwK7Tff2x42TNwtb%2B9eWzSFI%3D"
val config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"*/



val containerName = "containercggtest"
val storageAccountName = "storacccarlosgg"
/*val sas = "?sv=2019-12-12&ss=bfqt&srt=sco&sp=rwdlacupx&se=2020-11-25T00:08:08Z&st=2020-11-24T16:08:08Z&spr=https&sig=lovOaqukRy6RZP2JPKeyhJzepoxN2PMq8tMMauOVef0%3D"*/
val sas = "sv=2019-12-12&ss=bfqt&srt=sco&sp=rwdlacupx&se=2021-03-01T19:02:19Z&st=2020-11-25T11:02:19Z&spr=https&sig=xAcW2%2BTkleVuKC24vPs3YS8Xhvam4PCd%2FDEyQMVO1rI%3D"
val config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"


// COMMAND ----------

/*dbutils.fs.mount(
  source = "wasbs://azsqlshackcontainer@azsqlshackstorage.blob.core.windows.net/1000 Sales Records.csv",
  mountPoint = "/mnt/myfile",
  extraConfigs = Map(config -> sas))*/

dbutils.fs.mount(
  source = "wasbs://containercggtest@storacccarlosgg.blob.core.windows.net/TestCGG.csv",
  mountPoint = "/mnt/myfile3",
  extraConfigs = Map(config -> sas))




// COMMAND ----------

dbutils.fs.unmount("/mnt/myfile2")

// COMMAND ----------

dbutils.fs.unmount("/mnt/myfile")

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://containercggtest@storacccarlosgg.blob.core.windows.net/TestCGG.csv",
  mountPoint = "/mnt/myfile2",
  extraConfigs = Map(config -> sas))


// COMMAND ----------

val mydf = spark.read
.option("header","true")
.option("inferSchema", "true")
.csv("/mnt/myfile3")
display(mydf)



// COMMAND ----------

val mydf2 = spark.read
.option("header","true")
.option("inferSchema", "true")
.csv("/mnt/myfile3")
display(mydf2)

// COMMAND ----------

mydf2.createOrReplaceTempView("MisEmpleados")

// COMMAND ----------

/*jhu_daily_pop = spark.sql("""
SELECT f.FIPS, f.Admin2, f.Province_State, f.Country_Region, f.Last_Update, f.Lat, f.Long_, f.Confirmed, f.Deaths, f.Recovered, f.Active, f.Combined_Key, f.process_date, p.POPESTIMATE2019 
  FROM jhu_daily_covid f
    JOIN fips_popest_county p
      ON p.fips = f.FIPS
""")
jhu_daily_pop.createOrReplaceTempView("jhu_daily_pop")*/


val jhu_daily_pop = spark.sql("""SELECT *   FROM MisEmpleados""")

jhu_daily_pop.createOrReplaceTempView("jhu_daily_pop")



// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MisEmpleados

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MisEmpleados

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from 
// MAGIC jhu_daily_pop

// COMMAND ----------

/*%sql
CREATE TABLE MIsEmpleados
USING delta
AS SELECT *
FROM csv."""/mnt/myfile3/""*/

/*SET spark.databricks.delta.formatCheck.enabled=false*/

/*Answer by Abha · Aug 28 at 08:40 AM

1. using Spark SQL Context in python, scala notebooks :*/

sql("SET spark.databricks.delta.formatCheck.enabled=false")/* tuvr que poner esto para que no me chequease el formato DELTA*/

/*2. In SQL dbc notebooks:

SET spark.databricks.delta.formatCheck.enabled=false

Add comment · Share*/


dbutils.fs.mount(
  source = "wasbs://containercggtest@storacccarlosgg.blob.core.windows.net/MisEmpleadosDelta.csv",
  mountPoint = "/mnt/myfile4",
  extraConfigs = Map(config -> sas))


spark.sql("select * from jhu_daily_pop").write.format("delta").mode("overwrite").partitionBy("Color").save("/mnt/myfile4/")/*Esto funciono ; sobre el 3 no me iba*/

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS jhu_daily_pop_deltalake;
// MAGIC CREATE TABLE jhu_daily_pop_deltalake USING DELTA LOCATION '/mnt/myfile4/';
// MAGIC OPTIMIZE jhu_daily_pop_deltalake ZORDER BY (ID);

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from jhu_daily_pop_deltalake 

// COMMAND ----------

/*display(mydf)*/
import org.apache.spark.sql.functions

val selectspecificcolsdf = mydf.select("ID","Nombre","Color","Valor")/*Hace falta un espacio!! porque estaba asi*/
display(selectspecificcolsdf)
selectspecificcolsdf.createOrReplaceTempView("EmpleadosCGG")

// COMMAND ----------



val source_file = "/mnt/myfile3/TestCGG.csv"
  /*process_date = csv[100:104] + "-" + csv[94:96] + "-" + csv[97:99]*/
  
  /* Read data into temporary dataframe*/
  val df_tmp = spark.read.option("inferSchema", True).option("header", True).csv(source_file)
  df_tmp.createOrReplaceTempView("df_tmp")


// COMMAND ----------

// MAGIC %sql

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from EmpleadosCGG

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT Color, sum(Valor) From EmpleadosCGG
// MAGIC group by Color
// MAGIC -val diamonds = spark.sql("select * from diamonds")
// MAGIC display(diamonds.select("*"))
// MAGIC 
// MAGIC val diamonds = spark.table("diamonds")
// MAGIC display(diamonds.select("*"))order by Color

// COMMAND ----------

/*val diamonds = spark.sql("select * from diamonds")
display(diamonds.select("*"))

val diamonds = spark.table("diamonds")
display(diamonds.select("*"))
*/

val aggdata = spark.sql("SELECT Color, sum(Valor) From EmpleadosCGG group by Color")
display  (aggdata)

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://containercggtest@storacccarlosgg.blob.core.windows.net",
  mountPoint = "/mnt/resultado",
  extraConfigs = Map(config -> sas))

// COMMAND ----------

aggdata.write
 .option("header", "true")
 .format("com.databricks.spark.csv")
 .save("/mnt/resultado/EmpleadosCGG.csv")