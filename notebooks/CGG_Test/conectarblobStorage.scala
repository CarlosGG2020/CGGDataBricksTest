// Databricks notebook source
/*val containerName = "azsqlshackcontainer"
val storageAccountName = "azsqlshackstorage"
val sas = "?sv=2019-02-02&ss=b&srt=sco&sp=rwdlac&se=2020-03-30T07:53:28Z&st=2020-03-29T23:53:28Z&spr=https&sig=5vK%2FKEgTJVLoF4SX08IvwK7Tff2x42TNwtb%2B9eWzSFI%3D"
val config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"*/



val containerName = "containercggtest"
val storageAccountName = "storacccarlosgg"
val sas = "?sv=2019-12-12&ss=bfqt&srt=sco&sp=rwdlacupx&se=2020-11-25T00:08:08Z&st=2020-11-24T16:08:08Z&spr=https&sig=lovOaqukRy6RZP2JPKeyhJzepoxN2PMq8tMMauOVef0%3D"
val config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"


// COMMAND ----------

/*dbutils.fs.mount(
  source = "wasbs://azsqlshackcontainer@azsqlshackstorage.blob.core.windows.net/1000 Sales Records.csv",
  mountPoint = "/mnt/myfile",
  extraConfigs = Map(config -> sas))*/

dbutils.fs.mount(
  source = "wasbs://containercggtest@storacccarlosgg.blob.core.windows.net/TestCGG.csv",
  mountPoint = "/mnt/myfile",
  extraConfigs = Map(config -> sas))




// COMMAND ----------

val mydf = spark.read
.option("header","true")
.option("inferSchema", "true")
.csv("/mnt/myfile")
display(mydf)



// COMMAND ----------

/*display(mydf)*/
import org.apache.spark.sql.functions

val selectspecificcolsdf = mydf.select("ID","Nombre","Color","Valor")/*Hace falta un espacio!!*/
display(selectspecificcolsdf)
selectspecificcolsdf.createOrReplaceTempView("EmpleadosCGG")

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