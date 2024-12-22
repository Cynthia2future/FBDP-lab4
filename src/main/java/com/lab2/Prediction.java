package com.lab2;

import org.apache.spark.sql.expressions.Window;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Prediction {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Prediction")
                .master("local[*]") 
                .getOrCreate();

        Dataset<Row> data = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("/user/leizhengtian/chenqiaolei/user_balance_table.csv");

        data = data.selectExpr(
                "CAST(report_date AS INT) AS report_date",
                "CAST(total_purchase_amt AS BIGINT) AS total_purchase_amt",
                "CAST(total_redeem_amt AS BIGINT) AS total_redeem_amt"
        );

        // 按日期聚合计算每日申购与赎回总额
        Dataset<Row> dailyData = data.groupBy("report_date")
                .agg(functions.sum("total_purchase_amt").as("purchase"),
                     functions.sum("total_redeem_amt").as("redeem"))
                .orderBy("report_date");

        // 特征工程：添加日期特征
        dailyData = dailyData.withColumn("day_index", 
                functions.row_number().over(Window.orderBy("report_date")));

        // 训练集
        Dataset<Row> trainData = dailyData.filter("report_date <= 20140831");

        // 测试集
        Dataset<Row> testData = spark.range(20140901, 20140931)
                .withColumnRenamed("id", "report_date")
                .withColumn("day_index", functions.col("report_date").minus(20140534));  // 根据日期差值计算 day_index

        // 7. 构建特征向量
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"day_index"})
                .setOutputCol("features");

        trainData = assembler.transform(trainData).select("features", "purchase", "redeem");
        testData = assembler.transform(testData).select("features", "report_date");

        trainData.show();
        testData.show();

        // 8. 申购预测模型
        LinearRegression purchaseModel = new LinearRegression()
                .setLabelCol("purchase")
                .setFeaturesCol("features");
        LinearRegressionModel purchaseRegressor = purchaseModel.fit(trainData);

        // 预测申购
        Dataset<Row> purchasePredictions = purchaseRegressor.transform(testData)
                .select("report_date", "prediction")
                .withColumnRenamed("prediction", "predicted_purchase");

        // 9. 赎回预测模型
        LinearRegression redeemModel = new LinearRegression()
                .setLabelCol("redeem")
                .setFeaturesCol("features");
        LinearRegressionModel redeemRegressor = redeemModel.fit(trainData);

        // 预测赎回
        Dataset<Row> redeemPredictions = redeemRegressor.transform(testData)
                .select("report_date", "prediction")
                .withColumnRenamed("prediction", "predicted_redeem");

        // 10. 合并结果
        Dataset<Row> result = purchasePredictions.join(redeemPredictions, "report_date")
                .selectExpr("CAST(report_date AS BIGINT) AS report_date",
                            "ROUND(predicted_purchase) AS purchase",
                            "ROUND(predicted_redeem) AS redeem");

        result.write().option("header", "true")
              .csv("/user/leizhengtian/chenqiaolei/output4-2/");
        spark.stop();
    }
}
