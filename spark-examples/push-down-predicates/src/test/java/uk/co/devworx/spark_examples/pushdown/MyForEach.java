package uk.co.devworx.spark_examples.pushdown;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class MyForEach implements ForeachFunction<Row>, Serializable
{
	final AtomicInteger count = new AtomicInteger();

	@Override public void call(Row row) throws Exception
	{
		String o_instrument = row.getAs("O_INSTRUMENT");

		int x = count.incrementAndGet();

		if(x % 1000 == 0)
		{
			System.out.println("COUNT is now : " + x);
		}
	}
}