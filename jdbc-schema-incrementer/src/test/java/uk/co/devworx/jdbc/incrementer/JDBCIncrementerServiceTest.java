package uk.co.devworx.jdbc.incrementer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.Driver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Some test cases for the incrementer service
 */
public class JDBCIncrementerServiceTest
{
	private static final Logger logger = LogManager.getLogger(JDBCIncrementerServiceTest.class);

	final Path dbPath = Paths.get("target/test-database");
	final String jdbcURL = "jdbc:h2:" + dbPath.toFile().getAbsolutePath();
	final Connection con;
	final Map<String, String> envVariables;
	final String tableName;
	final String schemaName;
	final JDBCIncrementerService incrementerService;

	public JDBCIncrementerServiceTest() throws Exception
	{
		Stream<Path> paths = Files.walk(dbPath.getParent(), 0);
		paths.forEach(p ->
		{
			if(Files.isRegularFile(p))
			{
				try
				{
					logger.info("Deleting : " + p.toAbsolutePath());
					Files.delete(p);
				}
				catch(Exception e)
				{
					throw new RuntimeException("Encountered IO Exception : " + e);
				}
			}
		});

		logger.info("JDBC URL : " + jdbcURL);

		Class<?> drmv = Class.forName(Driver.class.getCanonicalName());
		logger.info("Driver Class : " + drmv);
		con = DriverManager.getConnection(jdbcURL, "sa", "");

		envVariables = new HashMap<>();
		envVariables.put("TARGET_DB","INFO");
		envVariables.put("NAMESPACE","test_");

		incrementerService = JDBCIncrementerService.getInstance(con, envVariables);
		tableName = incrementerService.replaceEnvs("${NAMESPACE}tbl_rainbow_schema");
		schemaName = incrementerService.replaceEnvs("${TARGET_DB}");

	}

	@BeforeEach
	public void resetDatabase() throws Exception
	{
		String schemaSQL = new String(Files.readAllBytes(Paths.get("src/test/resources/sql-schema/initial-setup.sql")));
		schemaSQL = incrementerService.replaceEnvs(schemaSQL);
		try(Statement stmt = con.createStatement())
		{
			logger.info("EXECUTING : " + schemaSQL);
			stmt.execute(schemaSQL);
			String sql = "SELECT * FROM " + schemaName + "." + tableName;
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + schemaName + "." + tableName);
			StringBuilder str = ResultSetUtils.toString(rs);
			logger.info(sql);
			logger.info("\n" + str);
		}
	}

	@Test
	public void testTableSchemaDescriptor()
	{
		logger.info("Executed : " + con);

		String tblColumns = incrementerService.getTableColumnsResultSetDebug(schemaName, tableName);
		logger.info(tblColumns);

		final Optional<TableSchemaDescriptor> descr = incrementerService.getTableSchemaDescriptor(schemaName, tableName);

		Assertions.assertNotNull(descr);
		Assertions.assertTrue(descr.isPresent());

		logger.info("TableSchemaDescriptor : \n" + descr.get());

		Map<String, TableColumn> allCols = descr.get().getColumns();
		allCols.forEach((k,v) ->
		{
			logger.info(k + " -> " + v);
		});

	}

	@Test
	public void testTableAppendedColumnsTransformSchemaDescriptor()
	{
		final TableSchemaTransform transform = incrementerService.getTransform(Paths.get("src/test/resources/sql-schema/01-appended-columns/tbl_rainbow_schema.sql"));
		Assertions.assertEquals(TableSchemaTransform.TransformType.TableMutation, transform.getTransformType());


	}



}















