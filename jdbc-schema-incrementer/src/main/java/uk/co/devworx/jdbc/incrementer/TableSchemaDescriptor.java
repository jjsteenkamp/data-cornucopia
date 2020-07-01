package uk.co.devworx.jdbc.incrementer;

import java.util.LinkedHashMap;
import java.util.SortedMap;

/**
 * Encapsulates the details associated with a table schema.
 *
 */
public class TableSchemaDescriptor
{
	private final LinkedHashMap<String, TableColumn> columns;
	private final String tableName;

	TableSchemaDescriptor(String tableName)
	{
		this.tableName = tableName;
		this.columns = new LinkedHashMap<>();
	}

	LinkedHashMap<String, TableColumn> getColumnsForMod()
	{
		return columns;
	}

}

class TableColumn
{
	private final String columnName;
	private final String sqlColumnTypeName;
	private final int sqlColumnType;

	TableColumn(String columnName, String sqlColumnTypeName, int sqlColumnType)
	{
		this.columnName = columnName;
		this.sqlColumnTypeName = sqlColumnTypeName;
		this.sqlColumnType = sqlColumnType;
	}
}
