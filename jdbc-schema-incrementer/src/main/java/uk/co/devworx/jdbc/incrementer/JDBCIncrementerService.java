package uk.co.devworx.jdbc.incrementer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.sql.*;
import java.util.*;

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
								   Map<String, String> envirronmentProperties) throws JDBCIncrementerServiceException
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
			throw new JDBCIncrementerServiceException("Unable to extract the database meta data : " + throwables, throwables);
		}

	}

	/**
	 *  The primary and most useful function in this implementation.
	 *  Given a create SQL statement (that conforms to the appropriate standards)
	 *  this will give you a Transform object that will describe
	 *  What steps / mutations need to be performed to get from the before to the after table.
	 *
	 *  The appropriate SQL steps can then be generated from that item.
	 *
	 * @return
	 */
	public TableSchemaTransform getTransform(Path sqlCreateStmt)
	{
		final CreateTableScriptsUtil createUtils = CreateTableScriptsUtil.getInstance(sqlCreateStmt);
		if(createUtils.hasErrors())
		{
			throw new JDBCIncrementerServiceException("Unable to introspect the create SQL script - " + createUtils.getErrorsReport());
		}

		String beforeTableName = createUtils.getTableName();
		String beforeTableSchema = createUtils.getSchemaName();

		TableSchemaDescriptor afterDescr = getTableSchemaDescriptorFromCreateScript(createUtils);
		Optional<TableSchemaDescriptor> beforeDescrOpt = getTableSchemaDescriptor(beforeTableSchema, beforeTableName, Optional.empty());

		if( beforeDescrOpt.isPresent() == true)
		{
			return new TableSchemaTransform(beforeDescrOpt.get(), afterDescr);
		}

		return new TableSchemaTransform(afterDescr);
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

	private ResultSet getTableColumnsResultSet(String schema, String tableName) throws JDBCIncrementerServiceException
	{
		try
		{
			ResultSet tables = dbMeta.getColumns(null, schema, tableName, null);
			return tables;
		}
		catch(Exception e)
		{
			throw new JDBCIncrementerServiceException("Unable to get the table schema descriptor : " + e, e);
		}
	}

	/**
	 * A handy debug function for getting tha table column result set.
	 *
	 * @param schema
	 * @param tableName
	 * @return
	 */
	public String getTableColumnsResultSetDebug(String schema, String tableName) throws JDBCIncrementerServiceException
	{
		final String tableNameToUse =  getTableName(tableName);
		final String schemaNameToUse =  getSchema(schema);

		try(ResultSet rs = getTableColumnsResultSet(schema, tableName))
		{
			StringBuilder sb = ResultSetUtils.toString(rs);
			sb.insert(0, "\nTable Name: " + tableNameToUse + "\nSchema Name: " + schemaNameToUse + "\n");
			return sb.toString();
		}
		catch(SQLException e)
		{
			String msg = "Encountered an SQL exception while trying to debug the schema.table - " + schemaNameToUse + "." + tableNameToUse + " -> " + e;
			logger.error(msg, e);
			throw new JDBCIncrementerServiceException(msg, e);
		}
	}

	/**
	 * Convenience overload for the other method of the same name;
	 *
	 * @param createScriptSql
	 */
	public TableSchemaDescriptor getTableSchemaDescriptorFromCreateScript(String createScriptSql)
	{
		final CreateTableScriptsUtil createUtil = CreateTableScriptsUtil.getInstance(createScriptSql);
		return getTableSchemaDescriptorFromCreateScript(createUtil);
	}


	/**
	 * Creates a schema descriptor from a create script.
	 *
	 * It does this by creating a table with generated name. Such that it does not clash with the original
	 *
	 * @return
	 */
	public TableSchemaDescriptor getTableSchemaDescriptorFromCreateScript(final CreateTableScriptsUtil createUtil) throws JDBCIncrementerServiceException
	{
		if(createUtil.getErrors().isEmpty() == false)
		{
			throw new RuntimeException(createUtil.getErrorsReport());
		}

		final CreateTableScriptsUtil.CreateTransientTableScript transientScript = createUtil.createTransientScript();

		try(final Statement stmt = con.createStatement())
		{
			final String fullyResolvedSchema = replaceEnvs(transientScript.getUnresolvedTransientTableSchema());
			final String fullyResolvedName = replaceEnvs(transientScript.getUnresolvedTransientTableName());
			final String fullyResolvedCreate = replaceEnvs(transientScript.getUnresolvedCreateTableScript());

			transientScript.setResolvedCreateTableScript(fullyResolvedCreate);
			transientScript.setResolvedTransientTableName(fullyResolvedName);
			transientScript.setResolvedTransientTableSchema(fullyResolvedSchema);

			logger.info("Creating a Transient Table - " + transientScript.getResolvedTransientTableName() + " - in order to get a descriptor...");

			stmt.execute(fullyResolvedCreate);
			logger.info("Created Transient Table - " + transientScript.getResolvedTransientTableName() + " - Now we can infer the descriptor..");

			final Optional<TableSchemaDescriptor> optSchema = getTableSchemaDescriptor(transientScript.getResolvedTransientTableSchema().get(),
																					   createUtil.getTableName(),
																					   transientScript.getResolvedTransientTableName().get(),
																					   Optional.of(transientScript)
			 																		  );
			if(optSchema.isPresent() == false)
			{
				throw new JDBCIncrementerServiceException("Unable to create a schema for the transient table : " + fullyResolvedName );
			}

			return optSchema.get();
		}
		catch(SQLException e)
		{
			String msg = "Encountered an SQL exception while attempting to set up the transient table - " + transientScript.getUnresolvedTransientTableName() + " : " + e;
			logger.error(msg, e);
			throw new JDBCIncrementerServiceException(msg, e);
		}

	}

	public Optional<TableSchemaDescriptor> getTableSchemaDescriptor(String schemaP,
																	String tableNameP,
																	Optional<CreateTableScriptsUtil.CreateTransientTableScript> transientCreateScriptP) throws JDBCIncrementerServiceException
	{
		return getTableSchemaDescriptor(schemaP, tableNameP, Optional.empty(),transientCreateScriptP);
	}

	public Optional<TableSchemaDescriptor> getTableSchemaDescriptor(String schemaP,
																	String tableNameP,
																	String transientNameP,
																	Optional<CreateTableScriptsUtil.CreateTransientTableScript> transientCreateScriptP) throws JDBCIncrementerServiceException
	{
		return getTableSchemaDescriptor(schemaP, tableNameP, Optional.of(transientNameP), transientCreateScriptP);
	}

	public Optional<TableSchemaDescriptor> getTableSchemaDescriptor(String schemaP,
																	String tableNameP,
																	Optional<String> transientNameP,
																	Optional<CreateTableScriptsUtil.CreateTransientTableScript> transientCreateScriptP) throws JDBCIncrementerServiceException
	{
		final String tableNameToUse =  getTableName(transientNameP.orElse(tableNameP));
		final Optional<String> transientNameToUse =  transientNameP.isPresent() ? Optional.of(getTableName(transientNameP.get())) : transientNameP;
		final String schema = getSchema(schemaP);

		if(isValidTableName(tableNameToUse) == false)
		{
			throw new JDBCIncrementerServiceException("You have not specified a valid table name : " + tableNameToUse);
		}

		try(final ResultSet tableDetails = getTableColumnsResultSet(schema, tableNameToUse))
		{
			if(tableDetails.next() == false)
			{
				return Optional.empty();
			}

			final TableSchemaDescriptor tblDescriptor = new TableSchemaDescriptor(schema,
																				  tableNameToUse,
																				  transientNameToUse,
																				  transientCreateScriptP);
			final List<TableColumn> columns = new ArrayList<>();

			do
			{
				final String colName = tableDetails.getString("COLUMN_NAME");
				final int colDataType = tableDetails.getInt("DATA_TYPE");
				final String colDataTypeName = tableDetails.getString("TYPE_NAME");
				final int ordinal = tableDetails.getInt("ORDINAL_POSITION");
				final boolean isNullable = tableDetails.getString("IS_NULLABLE").equalsIgnoreCase("YES");
				TableColumn tableColumn = new TableColumn(tblDescriptor, colName, colDataTypeName, colDataType, ordinal, isNullable);
				columns.add(tableColumn);
			}
			while(tableDetails.next());
			Collections.sort(columns);
			tblDescriptor.addColumns(columns);
			return Optional.of(tblDescriptor);
		}
		catch(Exception e)
		{
			throw new JDBCIncrementerServiceException("Unable to get the table schema descriptor : " + e, e);
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


