package uk.co.devworx.spark_examples.pushdown;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.UserDefinedType;

public class MyLocalTimeUDT extends UserDefinedType<DateType>
{
	@Override public DataType sqlType()
	{
		return DataTypes.DateType;
	}

	@Override public Object serialize(DateType obj)
	{
		return obj;
	}

	@Override public DateType deserialize(Object datum)
	{
		return (DateType)(datum);
	}

	@Override public Class userClass()
	{
		return MyLocalTime.class;
	}
}
