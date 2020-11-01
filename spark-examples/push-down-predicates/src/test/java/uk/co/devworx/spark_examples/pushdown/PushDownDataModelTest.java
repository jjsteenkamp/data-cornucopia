package uk.co.devworx.spark_examples.pushdown;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.UDTRegistration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import scala.Option;

import java.util.Collections;

public class PushDownDataModelTest
{

	@Test @Disabled
	public void testLocalDateTime()
	{
		UDTRegistration.register(MyLocalTime.class.getName(), MyLocalTimeUDT.class.getName());

		final SparkSession session = ExecutionEnv.getInstance().getSparkSession();

		Option<Class<?>> udtFor = UDTRegistration.getUDTFor(java.time.LocalTime.class.getName());

		System.out.println("UDF FOR : " + udtFor) ;

		final PushDownDataModel rnd = PushDownDataModel.getRandom();
		Encoder<PushDownDataModel> enc = Encoders.bean(PushDownDataModel.class);
		Dataset<PushDownDataModel> dataSet = session.createDataset(Collections.singletonList(rnd), enc);
		Dataset<Row> rowDataset = dataSet.toDF();

		rowDataset.schema().printTreeString();

		Dataset<PushDownDataModel> dataSet2 = dataSet.as(enc);


		dataSet.show();
		rowDataset.show();
		dataSet2.show();

		System.out.println(dataSet2 == dataSet);


	}


}
