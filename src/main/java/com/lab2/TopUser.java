package com.lab2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import static org.apache.spark.sql.functions.*;

public class TopUser {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("TopUser")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> userProfile = spark.read().option("header", "true").csv("/user/leizhengtian/chenqiaolei/user_profile_table.csv");
        Dataset<Row> userBalance = spark.read().option("header", "true").csv("/user/leizhengtian/chenqiaolei/user_balance_table.csv");

        userProfile = userProfile.withColumn("user_id", userProfile.col("user_id").cast("bigint"))
                                 .withColumn("City", userProfile.col("City").cast("bigint"));
        userBalance = userBalance.withColumn("user_id", userBalance.col("user_id").cast("bigint"))
                                 .withColumn("total_purchase_amt", userBalance.col("total_purchase_amt").cast("bigint"))
                                 .withColumn("total_redeem_amt", userBalance.col("total_redeem_amt").cast("bigint"))
                                 .withColumn("report_date", userBalance.col("report_date").cast("string"));

        // 筛选2014年8月的数据
        Dataset<Row> augustData = userBalance.filter("report_date >= '20140801' AND report_date <= '20140831'");

        // 计算总流量 (total_purchase_amt + total_redeem_amt)
        Dataset<Row> totalFlowData = augustData
                .withColumn("total_flow", col("total_purchase_amt").plus(col("total_redeem_amt")));

        // 关联user_profile表，获取城市信息
        Dataset<Row> joinedData = totalFlowData.join(userProfile, "user_id");

        // 按城市和用户ID分组，计算每个用户的总流量
        Dataset<Row> userTotalFlow = joinedData.groupBy("City", "user_id")
                .agg(sum("total_flow").alias("total_flow"));

        Dataset<Row> rankedData = userTotalFlow.withColumn("rank", 
                row_number().over(Window.partitionBy("City").orderBy(col("total_flow").desc())));

        Dataset<Row> TopUser = rankedData.filter("rank <= 3");
        Dataset<Row> result = TopUser.select("City", "user_id", "total_flow");
        
        result.show(100,false);
        spark.stop();
    }
}
