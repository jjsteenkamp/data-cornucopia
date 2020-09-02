package uk.co.devworx.spark_examples.pushdown;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;


import java.util.Properties;

public class SparkPushDownTransformerTest
{

	public static void main(String... args) throws Exception
	{
		SparkPushDownTransformerTest x = new SparkPushDownTransformerTest();
		x.test();
	}


	public void test() throws Exception
	{
		Thread.sleep(10_000);

		System.out.println("Before Start ");

		SparkSession sparkSession =	SparkSession .builder()
				.appName("SparkDataUtils")
				.master("local[1]")
				.config("spark.sql.orc.impl","native")
				.config("spark.sql.parquet.compression.codec", "gzip")
				.getOrCreate();

		System.out.println("After Start");

		Properties properties = new Properties();
		properties.put("user", "sa");
		properties.put("password", "");

		System.out.println("About to instantiate order events : ");

		String jdbc_url = SQLConnectionUtils.getJDBC_URL();

		Dataset<Row> df = sparkSession.read().jdbc(jdbc_url, "ORDERS_ORDER_EVENT_JOIN", properties);
		df.explain(true);

		System.out.println("Done instantiating");

		df.foreach(new MyForEach());
	}


}
