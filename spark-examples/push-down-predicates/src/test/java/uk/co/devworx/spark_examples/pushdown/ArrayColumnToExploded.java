package uk.co.devworx.spark_examples.pushdown;

import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A simple test to see how it is possible to expand an embedded array to a top level structure
 */
public class ArrayColumnToExploded
{

	@Test
	public void testExplode()
	{
		final SparkSession sparkSession = ExecutionEnv.getInstance().getSparkSession();

		final List<StructField> fieldList = new ArrayList<>();
		fieldList.add(DataTypes.createStructField("Name", DataTypes.StringType, true));
		fieldList.add(DataTypes.createStructField("Red", DataTypes.IntegerType, true));
		fieldList.add(DataTypes.createStructField("Green", DataTypes.IntegerType, true));
		fieldList.add(DataTypes.createStructField("Blue", DataTypes.IntegerType, true));

		StructType struct = DataTypes.createStructType(fieldList);

		Row r1 = RowFactory.create("Red", 255, 0, 0);
		Row r2 = RowFactory.create("Green", 0, 255, 0);
		Row r3 = RowFactory.create("Blue", 0, 0, 255);

		List<Row> colourList = new ArrayList<>();
		colourList.add(r1);
		colourList.add(r2);
		colourList.add(r3);

		Dataset<Row> ds = sparkSession.createDataFrame(colourList, struct);
		ds.show();

		ArrayType arrayType = DataTypes.createArrayType(struct);
		final StructType arrayStruct = DataTypes.createStructType(Collections.singletonList(DataTypes.createStructField("Colour", arrayType, true)));

		Object[][] compRow = new Object[1][];
		compRow[0] = new Object[]{r1,r2,r3};

		Row row = RowFactory.create(compRow);

		Dataset<Row> ds2 = sparkSession.createDataFrame(Collections.singletonList(row), arrayStruct);

		ds2.show();

		Column colourCol = ds2.col("Colour");

		Column exploded = functions.explode(colourCol);
		exploded = exploded.as("Colour");

		ExpressionEncoder<Row> encoder = RowEncoder.apply(struct);

		TypedColumn<Object, Row> typedCol = exploded.as(encoder);

		Dataset<Row> select = ds2.select(typedCol);

		select.printSchema();
		select.show();

		Dataset<Row> rowDataset = select.selectExpr("Colour.*");
		rowDataset.show();

	}

}
