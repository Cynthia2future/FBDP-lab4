package com.lab2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class UserBalanceAnalysis {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("UserBalanceAnalysis").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String inputPath = "/user/leizhengtian/chenqiaolei/user_balance_table.csv";
        JavaRDD<String> data = sc.textFile(inputPath);

        String header = data.first(); 
        JavaRDD<String> rows = data.filter((Function<String, Boolean>) line -> !line.equals(header));

        // 将数据映射为 (key: 日期, value: (资金流入量, 资金流出量))
        JavaPairRDD<String, Tuple2<Long, Long>> mappedData = rows.mapToPair((PairFunction<String, String, Tuple2<Long, Long>>) line -> {
            String[] fields = line.split(","); 
            String reportDate = fields[1];     
            long totalPurchaseAmt = Long.parseLong(fields[4]); 
            long totalRedeemAmt = Long.parseLong(fields[8]);   
            return new Tuple2<>(reportDate, new Tuple2<>(totalPurchaseAmt, totalRedeemAmt));
        });

        // 聚合每个日期的资金流入和流出量
        JavaPairRDD<String, Tuple2<Long, Long>> aggregatedData = mappedData
                .reduceByKey((tuple1, tuple2) -> new Tuple2<>(
                        tuple1._1 + tuple2._1, // 累加流入量
                        tuple1._2 + tuple2._2  // 累加流出量
                ));

        // 格式化输出
        JavaRDD<String> result = aggregatedData.map((Function<Tuple2<String, Tuple2<Long, Long>>, String>) tuple -> {
            String reportDate = tuple._1;  // 日期
            long totalPurchase = tuple._2._1; // 总流入量
            long totalRedeem = tuple._2._2;   // 总流出量
            return reportDate + " " + totalPurchase + " " + totalRedeem;
        });


        String outputPath = "/user/leizhengtian/chenqiaolei/output4-1";
        result.saveAsTextFile(outputPath);

        sc.stop();
    }
}