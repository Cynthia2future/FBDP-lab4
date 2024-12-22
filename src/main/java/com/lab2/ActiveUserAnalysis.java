package com.lab2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;

public class ActiveUserAnalysis {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ActiveUserAnalysis").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String inputPath = "/user/leizhengtian/chenqiaolei/user_balance_table.csv";
        JavaRDD<String> data = sc.textFile(inputPath);

        String header = data.first(); // 提取表头
        JavaRDD<String> rows = data.filter((Function<String, Boolean>) line -> !line.equals(header));

        // 设置目标日期范围为 2014年8月
        String startDate = "20140801";
        String endDate = "20140831";

        JavaPairRDD<Long, String> activeUserData = rows.mapToPair((PairFunction<String, Long, String>) line -> {
            String[] fields = line.split(",");
            long userId = Long.parseLong(fields[0]); 
            String reportDate = fields[1];             
            long directPurchaseAmt = Long.parseLong(fields[5]); 
            long totalRedeemAmt = Long.parseLong(fields[8]);   

            // 只处理2014年8月的记录
            if (reportDate.compareTo(startDate) >= 0 && reportDate.compareTo(endDate) <= 0) {
                boolean isActive = directPurchaseAmt > 0 || totalRedeemAmt > 0;
                if (isActive) {
                    return new Tuple2<>(userId, reportDate); // 返回用户ID和日期
                }
            }
            return null; // 不活跃用户不需要加入
        }).filter(tuple -> tuple != null);  

        // 对每个用户进行分组，统计活跃的天数
        JavaPairRDD<Long, Set<String>> userActiveDays = activeUserData
                .groupByKey()
                .mapValues(records -> {
                    Set<String> activeDates = new HashSet<>();
                    for (String date : records) {
                        activeDates.add(date); 
                    }
                    return activeDates;
                });

        // 筛选出活跃天数大于等于5天的用户
        JavaRDD<Long> activeUsers = userActiveDays.filter(tuple -> tuple._2.size() >= 5).keys();
        long activeUserCount = activeUsers.count();
        System.out.println("活跃用户总数: " + activeUserCount);

        sc.stop();
    }
}