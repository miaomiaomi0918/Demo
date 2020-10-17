package com.bdqn

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadToMysql extends App {

  val spark = SparkSession.builder
    .master("local[2]")
    .appName("appName")
    .enableHiveSupport()
    .getOrCreate()
  val sc=spark.sparkContext
  import spark.implicits._

  private val sourceDF: DataFrame = spark.read.format("csv").option("header", "true").load(".idea\\data\\student_score.csv")
  sourceDF.show(20)
  val tableName2 = sourceDF.createOrReplaceTempView("student_score")
  val url = "jdbc:mysql://localhost:3306/l_lunches"
  val tableName = "student_score"
  // 设置连接用户、密码、数据库驱动类
  val prop = new java.util.Properties
  prop.setProperty("user","root")
  prop.setProperty("password","123456")
  prop.setProperty("driver","com.mysql.jdbc.Driver")
//  sourceDF.write.mode("append").jdbc(url,"student_score",prop)


  private val df: DataFrame = spark.sql(
    """
SELECT
stu_id,
sum(cou_hadoop),
sum(cou_hive),
sum(cou_spark),
round(sum(cou_hadoop)+sum(cou_hive)+sum(cou_spark)/3,2) avg_score,
sum(cou_hadoop)+sum(cou_hive)+sum(cou_spark) total_score,
rank() over(distribute by `time` sort by sum(cou_hadoop)+sum(cou_hive)+sum(cou_spark) desc) rn,
`time`
from(
SELECT
stu_id,
if(cou_id="1" and score != "null",score,0) cou_hadoop,
0 cou_hive,
0 cou_spark,
`time`
FROM student_score
where cou_id="1"
union all
SELECT
stu_id,
0 cou_hadoop,
if(cou_id="2" and score != "null",score,0) cou_hive,
0 cou_spark,
`time`
FROM student_score
where cou_id="2"
union ALL
SELECT
stu_id,
0 cou_hadoop,
0 cou_hive,
if(cou_id="3" and score != "null",score,0) cou_spark,
`time`
FROM student_score
where cou_id="3") t
group by t.stu_id,t.`time`
  """.stripMargin)
  private val tablename3: Unit = df.createGlobalTempView("student_score_detail_month")
//  df.write.mode("append").jdbc(url,"student_score_detail_month",prop)
}
