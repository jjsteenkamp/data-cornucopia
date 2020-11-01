package uk.co.devworx.spark_examples.pushdown;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkPushDownTransformerTest
{
	private static final Logger logger = LogManager.getLogger(SparkPushDownTransformerTest.class);

	public static void main(String... args) throws Exception
	{
		SparkPushDownTransformerTest x = new SparkPushDownTransformerTest();
		x.test();
	}


	public void test() throws Exception
	{
		logger.info("Before Start ");
		final SparkSession sparkSession = ExecutionEnv.getInstance().getSparkSession();

		logger.info("After Start");



		logger.info("About to instantiate order events : ");

		String jdbc_url = SQLConnectionUtils.getJDBC_URL();

		Dataset<Row> df = sparkSession.read().jdbc(jdbc_url, "ORDERS_ORDER_EVENT_JOIN", SQLConnectionUtils.getProperties());
		df.explain(true);

		logger.info("Done instantiating");

		df.foreach(new MyForEach());
	}


}
