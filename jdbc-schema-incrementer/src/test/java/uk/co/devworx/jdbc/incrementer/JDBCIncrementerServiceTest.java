package uk.co.devworx.jdbc.incrementer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.Driver;
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

		String schemaSQL = new String(Files.readAllBytes(Paths.get("src/test/resources/sql-schema/tbl_rainbow_schema.sql")));
		schemaSQL = incrementerService.replaceEnvs(schemaSQL);

		Statement stmt = con.createStatement();

		logger.info("EXECUTING : " + schemaSQL);

		stmt.execute(schemaSQL);

		tableName = incrementerService.replaceEnvs("${NAMESPACE}tbl_rainbow_schema");
		schemaName = incrementerService.replaceEnvs("${TARGET_DB}");

		String sql = "SELECT * FROM " + schemaName + "." + tableName;

		ResultSet rs = stmt.executeQuery("SELECT * FROM " + schemaName + "." + tableName);

		StringBuilder str = ResultSetUtils.toString(rs);
		logger.info(sql);
		logger.info("\n" + str);

	}

	@Test
	public void testTableSchemaDescriptor()
	{
		logger.info("Executed : " + con);

		final TableSchemaDescriptor descr = incrementerService.getTableSchemaDescriptor(schemaName, tableName);

	}



}















