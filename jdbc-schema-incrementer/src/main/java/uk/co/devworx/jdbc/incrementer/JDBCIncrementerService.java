package uk.co.devworx.jdbc.incrementer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

/**
 * A service class to deal with the various JDBC incrementer functions.
 *
 */
public class JDBCIncrementerService
{
	private static final Logger logger = LogManager.getLogger(JDBCIncrementerService.class);

	/**
	 * A prefix that will be used for the creation of 'throw-away' tables
	 */
	public static final String TEMP_TABLE_PREFIX = "tmp_";

	public static final String ENV_REPLACE_FORMAT = "${%s}";

	public static final String SCHEMA_SEPARATOR = ".";

	/**
	 * Gets an incrementer service from a database connection
	 * @return
	 */
	public static JDBCIncrementerService getInstance(Connection con,
													 Map<String, String> envirronmentProperties)
	{
		return new JDBCIncrementerService(con, envirronmentProperties);
	}

	private final Connection con;
	private final DatabaseMetaData dbMeta;
	private final Map<String, String> envirronmentProperties;

	private final boolean lowerCaseIdentifiers;
	private final boolean upperCaseIdentifiers;

	private JDBCIncrementerService(Connection con,
								   Map<String, String> envirronmentProperties)
	{
		this.con = con;
		this.envirronmentProperties = envirronmentProperties;

		try
		{
			dbMeta = con.getMetaData();
			lowerCaseIdentifiers = dbMeta.storesLowerCaseIdentifiers();
			upperCaseIdentifiers = dbMeta.storesUpperCaseIdentifiers();
		}
		catch (SQLException throwables)
		{
			throw new RuntimeException("Unable to extract the database meta data : " + throwables, throwables);
		}

	}

	/**
	 * Gets the appropriate table name from the given table name.
	 * Takes care of environment replacements, upper case, lower case etc.
	 *
	 * @param tableName
	 * @return
	 */
	public String getTableName(String tableName)
	{
		String res = replaceEnvs(tableName);
		if(lowerCaseIdentifiers == true) return res.toLowerCase();
		if(upperCaseIdentifiers == true) return res.toUpperCase();
		return res;
	}

	public boolean isValidTableName(String tableName)
	{
		if(tableName.contains(SCHEMA_SEPARATOR) == true)
		{
			return false;
		}
		return true;
	}

	public TableSchemaDescriptor getTableSchemaDescriptor(String schemaP, String tableNameP)
	{
		final String tableName = getTableName(tableNameP);
		final String schema = getSchema(schemaP);
		if(isValidTableName(tableName) == false)
		{
			throw new RuntimeException("You have not specified a valid table name : " + tableName);
		}

		try
		{
			final DatabaseMetaData dbMetaData = con.getMetaData();

			ResultSet tables = dbMetaData.getTables(null, schema, tableName, null);
			logger.info("Table = " + tableName + ", Schema = " + schema + " \n" + ResultSetUtils.toString(tables));

			ResultSet tableCols = dbMetaData.getColumns(null, null, tableName.toUpperCase(), null);
			logger.info("Table = " + tableName + ", Schema = " + schema + " \n" + ResultSetUtils.toString(tableCols));

			return null;
		}
		catch(Exception e)
		{
			throw new RuntimeException("Unable to get the table schema descriptor : " + e, e);
		}
	}

	private String getSchema(String schemaP)
	{
		String res = replaceEnvs(schemaP);
		if(lowerCaseIdentifiers == true) return res.toLowerCase();
		if(upperCaseIdentifiers == true) return res.toUpperCase();
		return res;
	}

	/**
	 * A very simple rudimentary environment replacement tool.
	 *
	 * @param input
	 * @return
	 */
	public String replaceEnvs(final String input)
	{
		Set<Map.Entry<String, String>> set = envirronmentProperties.entrySet();
		String res = input;
		for(Map.Entry<String, String> e : set)
		{
			String property = String.format(ENV_REPLACE_FORMAT, e.getKey());
			if(res.contains(property))
			{
				res = res.replace(property, e.getValue());
			}
		}
		return res;
	}



}


