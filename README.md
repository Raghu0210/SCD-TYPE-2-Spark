# SCD-TYPE-2-Spark


import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.{ SparkConf, SparkContext }
import java.sql.{Date, Timestamp}
import org.apache.spark.sql.types.{ StructType, StructField, StringType, DoubleType, IntegerType,TimestampType }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, lit, when,concat}
import org.apache.spark.sql.expressions.Window

/*
-------------------START------------------
-------------------START------------------
-------------------START------------------
LOAD THE MASTER DATA FILE [SCHEMA - id, name, dept, city, etl_crtd, eff_start_dttm, eff_end_dttm, current_flag, checksum_nbr, surrogate_key]
PK - id
TS(TimeStamp) - etl_crtd
FORMAT - Parquet           */

val masterDF = spark.read.parquet("/FileStore/tables/StagingData")



//GET MAX SURROGATE KEY VALUE FOR FUTURE REFERENCE

val maxSurrogate = masterDF.select("surrogate_key").agg(max("surrogate_key"))

val maxvalCollect = maxSurrogate.as[Integer].collect()

var maxValue = maxvalCollect(0)



/*
LOAD THE DELTA DATA FILE [SCHEMA - id, name, dept, city, etl_crtd]
PK - id
TS(TimeStamp) - etl_crtd
FORMAT - CSV               */

val deltaDF = spark.read
      .format("csv")
      .option("header", "true")                                              
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("charset", "UTF-8")
      .option("escape", "\"")
      .load("/FileStore/tables/DeltaType2.csv")

/*Adding HASH value (chck_sum)
-- NOT inclusing the PK and TS columns while taking checksum -- */
val hashColumns = Array("name", "dept", "city")
val deltaHashDF = deltaDF.withColumn("checksum_nbr", hash(upper(concat(hashColumns.map(c => when(col(c).isNotNull, col(c)).otherwise(lit(""))): _*))))



//STEP 1 : Identify which records are going to expire in STAGING and move it to N status AND Identify the updates [2 TASKS]

//masterDF -- STAGING DATA
//deltaHashDF -- DELTA DATA

val step1 =  masterDF.join(deltaHashDF , masterDF("id") === deltaHashDF("id") && masterDF("current_flag") === "Y" && masterDF("checksum_nbr") =!= deltaHashDF("checksum_nbr"),"inner")


// Get the column arrangement(as reference) and use it whenever required to rearrange the columns

val formattedCol = masterDF.columns



//[Task 1] - Expired records

val selectColumns = masterDF.columns.map(masterDF(_)) ++ Array($"new_eff_end_dttm",$"new_current_flag")

val recordsExpired_Stg = step1.withColumn("timeStamp_eff_end_dttm",(((deltaHashDF("etl_crtd")).cast("timestamp") - expr("INTERVAL 1 SECOND"))))
                      .withColumn("new_current_flag",regexp_replace(masterDF("current_flag"),"Y","N"))
                      .withColumn("new_eff_end_dttm",col("timeStamp_eff_end_dttm").cast("String"))
                      .select(selectColumns:_*)
                      .drop("current_flag","eff_end_dttm")
                      .withColumnRenamed("new_current_flag","current_flag")
                      .withColumnRenamed("new_eff_end_dttm","eff_end_dttm")

val exipredRecords = recordsExpired_Stg.selectExpr(formattedCol:_*)



//[TASK - 2] Identify the Delta updates and assign new incremented Surrogate key

val selectColumnsDelta = deltaHashDF.columns.map(deltaHashDF(_))  ++ Array($"Delta_eff_start_dttm",$"Delta_eff_end_dttm",$"Delta_current_flag",$"Delta_surrogate_key")

val selectColDelta = deltaHashDF.columns.map(deltaHashDF(_))

val updateFromDelta = step1.select(selectColDelta:_*)
                        .withColumn("eff_start_dttm",deltaHashDF("etl_crtd"))
                        .withColumn("eff_end_dttm",lit("2999-01-01 01:01:01"))                 
                        .withColumn("current_flag",lit("Y"))
                        .withColumn("surrogate_key",(row_number().over(Window.orderBy(lit(1)))) +maxValue)
                        
val deltaUpdates = updateFromDelta.selectExpr(formattedCol:_*)




// Get revised max value for Surrogate key

if(!deltaUpdates.isEmpty){
  
val revisedMaxValue = deltaUpdates.select("surrogate_key").agg(max("surrogate_key"))

val newMaxCollect = revisedMaxValue.as[Integer].collect()

maxValue = newMaxCollect(0)
  
}



//Unaffected active records in Staging

val step2a = masterDF.where("current_flag = 'Y'").join(deltaHashDF , masterDF("id") === deltaHashDF("id")  && masterDF("checksum_nbr") =!= deltaHashDF("checksum_nbr"), "left_anti")

val step2b = step2a.selectExpr(formattedCol:_*)




//Filter out the history from STAGING, flag == "N"

val step3 = masterDF.filter("current_flag  == 'N'" )



//INSERT


val selectColumnsInsert = deltaDF.columns.map(deltaDF(_)) ++ Array($"checksum_nbr",$"eff_start_dttm",$"new_eff_end_dttm",$"new_current_flag",$"Delta_surrogate_key")

val step6 = deltaHashDF.join(masterDF , masterDF("id") === deltaHashDF("id") , "left_anti")
                      .withColumn("checksum_nbr", hash(upper(concat(hashColumns.map(c => when(col(c).isNotNull, col(c)).otherwise(lit(""))): _*))))
                      .withColumn("eff_start_dttm",deltaHashDF("etl_crtd"))
                      .withColumn("new_eff_end_dttm",lit("2999-01-01 01:01:01"))  
                      .withColumn("Delta_surrogate_key",(row_number().over(Window.orderBy(lit(1)))) +maxValue)
                      .withColumn("new_current_flag",lit("Y"))
                      .select(selectColumnsInsert:_*)
                      .withColumnRenamed("new_current_flag","current_flag")
                      .withColumnRenamed("new_eff_end_dttm","eff_end_dttm")
                      .withColumnRenamed("Delta_surrogate_key","surrogate_key")

val insertFromDelta = step6.selectExpr(formattedCol:_*)



//Final Output --> Union all the obtained result

val unionAllResults = deltaUpdates.union(step2b).union(exipredRecords).union(step3).union(insertFromDelta)


unionAllResults.write.parquet("/FileStore/tables/StagingData")

