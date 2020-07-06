package uk.co.devworx.jdbc.incrementer;

import java.util.Map;
import java.util.SortedSet;

/**
 * A Class that will help with the conversion of data types back and forth.
 *
 */
public class SQLDataTypeHelper
{
	public static final String STRING_TYPE = "VARCHAR";

	/**
	 * Checks to see if any of the type changes on the given table may have some sort of incompatibility
	 * going to the new structure.
	 *
	 * @param after
	 * @param columnsWithChangedTypes
	 * @return
	 */
	public static boolean areChangedColumnTypesFailurePotentials(final TableSchemaDescriptor after,
																 final SortedSet<TableColumn> columnsWithChangedTypes)
	{
		Map<String, TableColumn> newCols = after.getColumns();
		boolean failurePotential = false;
		for(TableColumn col : columnsWithChangedTypes)
		{
			TableColumn tableColumn = newCols.get(col.getColumnName());
			if(tableColumn == null) continue;
			failurePotential = isFailurePotential(col.getSqlColumnTypeName(), tableColumn.getSqlColumnTypeName());
			if(failurePotential == true)
			{
				break;
			}
		}
		return failurePotential;
	}

	public static String generateColumnTransformCode(TableColumn before, TableColumn after)
	{
		return "CAST( " + before.getColumnName() + " AS " + after.getSqlColumnTypeName() + " ) ";
	}

	public static boolean isFailurePotential(String sqlTypeFrom, String sqlTypeTo)
	{
		if(sqlTypeTo.equals(STRING_TYPE) == true)
		{
			return false;
		}
		return true;
	}

}
