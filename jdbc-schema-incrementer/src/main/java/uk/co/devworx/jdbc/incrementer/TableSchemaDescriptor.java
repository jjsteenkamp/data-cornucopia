package uk.co.devworx.jdbc.incrementer;

import java.util.*;

/**
 * Encapsulates the details associated with a table schema.
 *
 */
public class TableSchemaDescriptor
{
	private final LinkedHashMap<String, TableColumn> columns;
	private final String schema;
	private final String rootName;

	/**
	 * Specfied if this is a transient table.
	 */
	private final Optional<String> transientName;

	TableSchemaDescriptor(String schema,
						  String rootName,
						  Optional<String> transientName)
	{
		this.rootName = rootName;
		this.transientName = transientName;
		this.schema = schema;
		this.columns = new LinkedHashMap<>();
	}


	public Map<String, TableColumn> getColumns()
	{
		return Collections.unmodifiableMap(columns);
	}

	public void addColumns(List<TableColumn> cs)
	{
		cs.forEach(t ->
	    {
			TableColumn prevValue = columns.put(t.getColumnName(), t);
			if(prevValue != null) throw new IllegalArgumentException("You should not have the duplicate column mapping : " + t.getColumnName());
		});
	}

	@Override public String toString()
	{
		return "TableSchemaDescriptor{" + "column count=" + columns.size() + ", schema='" + schema + '\'' + ", rootName='" + rootName + '\'' + ", transientName=" + transientName + '}';
	}

	public String getTitle()
	{
		return schema + "." + rootName + "(transient_name=" + transientName.orElse("<none>") + ")";
	}

}

class TableColumn implements Comparable<TableColumn>
{
	private final TableSchemaDescriptor parent;
	private final String columnName;
	private final String sqlColumnTypeName;
	private final int sqlColumnType;
	private final int ordinal;
	private final boolean isNullable;

	TableColumn(TableSchemaDescriptor parent,
				String columnName,
				String sqlColumnTypeName,
				int sqlColumnType,
				int ordinal,
				boolean isNullable)
	{
		this.parent = parent;
		this.columnName = columnName;
		this.sqlColumnTypeName = sqlColumnTypeName;
		this.sqlColumnType = sqlColumnType;
		this.ordinal = ordinal;
		this.isNullable = isNullable;
	}

	@Override public int compareTo(TableColumn o)
	{
		int c1 = ordinal;
		int c2 = o.ordinal;
		if(c1 > c2) return 1;
		if(c1 < c2) return -1;
		return 0;
	}

	public String getColumnName()
	{
		return columnName;
	}

	@Override public String toString()
	{
		return "TableColumn{" + "parentName=" + parent.getTitle() + ", columnName='" + columnName + '\'' + ", sqlColumnTypeName='" + sqlColumnTypeName + '\'' + ", sqlColumnType=" + sqlColumnType + ", ordinal=" + ordinal + ", isNullable=" + isNullable + '}';
	}
}
