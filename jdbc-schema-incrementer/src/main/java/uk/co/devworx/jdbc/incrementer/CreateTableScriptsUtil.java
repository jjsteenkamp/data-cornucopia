package uk.co.devworx.jdbc.incrementer;

import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In order for the auto database incrementer to work, it is essential
 * that all the "CREATE TABLE" scripts confirm to a certain standard.
 * Such that we are able to "parse" it and get out.
 *
 * In particular, the script needs to confirm to the following structure:
 *
 * DROP TABLE IF EXISTS schema_name.table_name;
 *
 * CREATE TABLE schema_name.table_name
 * (
 *  ....
 * )... ;
 *
 * If not conforming to this structure, the tool will not be able to create
 * the transient table from which to infer the schemas.
 *
 */
public class CreateTableScriptsUtil
{
	private final String createTableScript;

	private final Optional<String> schemaName;
	private final Optional<String> tableName;
	private final Optional<String> dropTableLine;
	private final Optional<String> createTableSection;

	/**
	 * Holder for the script objects/
	 */
	public static class CreateTransientTableScript
	{
		private final String transientTableName;
		private final String transientTableSchema;
		private final String createTableScript;

		private final AtomicReference<String> resolvedCreateTableScript = new AtomicReference<>();
		private final AtomicReference<String> resolvedTransientTableName = new AtomicReference<>();
		private final AtomicReference<String> resolvedTransientTableSchema = new AtomicReference<>();

		CreateTransientTableScript(final String transientTableSchema,
								   final String transientTableName,
								   final String createTableScript)
		{
			this.transientTableSchema = transientTableSchema;
			this.transientTableName = transientTableName;
			this.createTableScript = createTableScript;
		}


		public String getUnresolvedTransientTableName()
		{
			return transientTableName;
		}

		void setResolvedCreateTableScript(String resolvedScript)
		{
			resolvedCreateTableScript.set(resolvedScript);
		}

		void setResolvedTransientTableSchema(String resolvedScript)
		{
			resolvedTransientTableSchema.set(resolvedScript);
		}

		void setResolvedTransientTableName(String resolvedScript)
		{
			resolvedTransientTableName.set(resolvedScript);
		}

		public Optional<String> getResolvedTransientTableSchema()
		{
			if(resolvedTransientTableSchema.get() == null) return Optional.empty();
			return Optional.of(resolvedTransientTableSchema.get());
		}

		public Optional<String> getResolvedTransientTableName()
		{
			if(resolvedTransientTableName.get() == null) return Optional.empty();
			return Optional.of(resolvedTransientTableName.get());
		}

		public Optional<String> getResolvedCreateTableScript()
		{
			if(resolvedCreateTableScript.get() == null)
			{
				return Optional.empty();
			}
			return Optional.of(resolvedCreateTableScript.get());
		}



		public String getUnresolvedCreateTableScript()
		{
			return createTableScript;
		}

		public String getUnresolvedTransientTableSchema()
		{
			return transientTableSchema;
		}
	}

	public static CreateTableScriptsUtil getInstance(Path scriptLocation)
	{
		try
		{
			String sql = new String(Files.readAllBytes(scriptLocation));
			return new CreateTableScriptsUtil(sql);
		}
		catch(Exception e)
		{
			throw new RuntimeException("Unable to read from the script location : " + e, e);
		}
	}

	public static CreateTableScriptsUtil getInstance(String sqlScript)
	{
		return new CreateTableScriptsUtil(sqlScript);
	}

	private CreateTableScriptsUtil(String createTableScript)
	{
		this.createTableScript = createTableScript;
		final List<String> lines = new ArrayList<>();

		Optional<String> pre_dropTableLine = Optional.empty();
		Optional<String> pre_createTableLine = Optional.empty();

		try(final BufferedReader bufReader = new BufferedReader(new StringReader(createTableScript)))
		{
			String line = null;
			boolean capturingCreateStatement = false;
			final StringBuilder crstmt = new StringBuilder();
			while((line = bufReader.readLine()) != null)
			{
				final String trimmed = line.trim();
				if(trimmed.equals("")) continue;
				if(trimmed.startsWith("--")) continue;

				if(capturingCreateStatement == true)
				{
					crstmt.append(trimmed);
					crstmt.append("\n");
					if(trimmed.endsWith(";"))
					{
						capturingCreateStatement = false;
						pre_createTableLine = Optional.of(crstmt.toString());
						continue;
					}
				}
				else
				{
					String trimmedUpperCase = trimmed.toUpperCase();
					if(trimmedUpperCase.startsWith("DROP TABLE IF EXISTS") == true)
					{
						pre_dropTableLine = Optional.of(trimmed);
						continue;
					}

					if(trimmedUpperCase.startsWith("CREATE TABLE "))
					{
						crstmt.append(trimmed);
						crstmt.append("\n");
						capturingCreateStatement = true;
						continue;
					}
				}
			}
		}
		catch(Exception e)
		{
			throw new RuntimeException("Unexpected I/O Exception for reading the table script : " + e, e);
		}

		if(pre_createTableLine.isPresent() == true)
		{
			createTableSection = pre_createTableLine;
			String val = pre_createTableLine.get();
			String[] lineSplitter = val.substring(0, val.indexOf("\n")).split(" ");
			StringBuilder fqTableName = new StringBuilder();
			for(String x : lineSplitter)
			{
				String xtrim = x.trim();
				if(xtrim.equals("")) continue;
				if(xtrim.toUpperCase().equals("CREATE")) continue;
				if(xtrim.toUpperCase().equals("TABLE")) continue;
				fqTableName.append(x.trim());
			}

			int dotIndex = fqTableName.indexOf(".");
			if(dotIndex != -1)
			{
				String sn = fqTableName.substring(0, dotIndex);
				String tbl = fqTableName.substring(dotIndex + 1);

				if(tbl.contains("("))
				{
					tbl = tbl.substring(0, tbl.indexOf("(")).trim();
				}

				if(tbl.contains(" "))
				{
					tbl = tbl.substring(0, tbl.indexOf(" ")).trim();
				}

				if(sn.length() > 0)
				{
					schemaName = Optional.of(sn);
				}
				else
				{
					schemaName = Optional.empty();
				}
				if(tbl.length() > 0)
				{
					tableName = Optional.of(tbl);
				}
				else
				{
					tableName = Optional.empty();
				}
			}
			else
			{
				schemaName = Optional.empty();
				tableName = Optional.empty();
			}
		}
		else
		{
			createTableSection = Optional.empty();
			schemaName = Optional.empty();
			tableName = Optional.empty();
		}
		dropTableLine = pre_dropTableLine;
	}

	/**
	 * Returns if this particular class has any errors.
	 *
	 * @return
	 */
	public boolean hasErrors()
	{
		return getErrors().isEmpty() == false;
	}


	/**
	 * Gets the errors associated with this create script.
	 * If there are no errors, an empty list is returned.
	 *
	 * @return
	 */
	public List<String> getErrors()
	{
		List<String> errors = new ArrayList<>();

		if(createTableSection.isPresent() == false)
		{
			errors.add("Unable to determine the CREATE TABLE statement from the supplied script.");
		}

		if(dropTableLine.isPresent() == false)
		{
			errors.add("Unable to determine the DROP TABLE IF EXISTS statement from the supplied script.");
		}

		if(tableName.isPresent() == false)
		{
			errors.add("The system was unable to infer a table name from the script you specified. The Create table script line was : " + createTableSection.orElse("<none>"));
		}

		if(schemaName.isPresent() == false)
		{
			errors.add("The system was unable to infer a schame name from the script you specified. The Create table script line was : " + createTableSection.orElse("<none>"));
		}

		return Collections.unmodifiableList(errors);
	}

	public String getErrorsReport()
	{
		final List<String> errors = getErrors();
		if(errors.isEmpty() == true) return "";
		StringBuilder items = new StringBuilder();
		items.append("Create Script Errors: \n");
		for(String e : errors)
		{
			items.append(e);
			items.append("\n");
		}
		return items.toString();
	}

	/**
	 * Returns a suffix that is database safe that can be used for transient tables.
	 */
	public static String getTransientTableSuffix()
	{
		String val = LocalDateTime.now().toString();
		val = val.replace(":","_");
		val = val.replace("-","_");
		val = val.replace("T","_");
		val = val.replace(".","_");
		return val;
	}

	public CreateTransientTableScript createTransientScript()
	{
		if(hasErrors() == true)
		{
			throw new IllegalStateException("You are not in the correct state in order to generate a transient script. " + getErrorsReport() );
		}

		final String transientName = 	JDBCIncrementerService.TEMP_TABLE_PREFIX + "_" + getTableName() + "_" + getTransientTableSuffix();
		final String fqName = getSchemaName() + "." + getTableName();
		final String fqTransientName = getSchemaName() + "." + transientName;
		final String newCreateTable = createTableSection.get().replace(fqName, fqTransientName);
		return new CreateTransientTableScript(getSchemaName(), transientName, newCreateTable);
	}

	public String getCreateTableScript()
	{
		return createTableScript;
	}

	public String getSchemaName()
	{
		return schemaName.orElseThrow(() -> new IllegalStateException("No schema name could be found - check the errors list"));
	}

	public String getTableName()
	{
		return tableName.orElseThrow(() -> new IllegalStateException("No schema name could be found - check the errors list"));
	}

	public Optional<String> getDropTableLine()
	{
		return dropTableLine;
	}

	public Optional<String> getCreateTableSection()
	{
		return createTableSection;
	}
}
