package com.lab2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jline.terminal.impl.jna.freebsd.CLibrary.winsize;

public class CitytBalance {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("CityBalance")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> userProfile =spark.read().option("header","true").csv("/user/leizhengtian/chenqiaolei/user_profile_table.csv");
        Dataset<Row> userBalance =spark.read().option("header","true").csv("/user/leizhengtian/chenqiaolei/user_balance_table.csv");
        

        userProfile = userProfile.withColumn("user_id", userProfile.col("user_id").cast("bigint"))
        .withColumn("City", userProfile.col("City").cast("bigint"));
        userBalance = userBalance.withColumn("user_id", userBalance.col("user_id").cast("bigint"))
                                 .withColumn("tBalance", userBalance.col("tBalance").cast("bigint"))
                                 .withColumn("report_date", userBalance.col("report_date").cast("string"));

        userProfile.createOrReplaceTempView("user_profile");
        userBalance.createOrReplaceTempView("user_balance");

        String query = "SELECT u.City, AVG(b.tBalance) as avg_balance " +
                       "FROM user_balance b " +
                       "JOIN user_profile u ON b.user_id = u.user_id " +
                       "WHERE b.report_date = '20140301' " +
                       "GROUP BY u.City " +
                       "ORDER BY avg_balance DESC";

        Dataset<Row> result = spark.sql(query);
        result.show();
        spark.stop();
    }
}