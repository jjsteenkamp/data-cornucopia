package uk.co.devworx.jdbc.incrementer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

/**
 * 
 * @author jsteenkamp
 */
public class SQLRunnerTest
{
	private static final Logger logger = LogManager.getLogger(SQLRunnerTest.class);
	
	@Test
	public void testLoadDatabaseAndExecute() throws Exception
	{
		Path p = Paths.get("target/SQLRunnerTest");
		Files.createDirectories(p);
		
		String jdbcURL = "jdbc:h2:./" + p.toString() + "/db";
		Connection conn = DriverManager.getConnection(jdbcURL);
		
		logger.info("Connecting to : " + jdbcURL);
		
		Statement stmt = conn.createStatement();
		
		logger.info("Setting up the database");
		
		stmt.execute("CREATE TABLE GLA_SURVEY_SOURCE AS SELECT * FROM CSVREAD('src/test/resources/test-data-set/gla-als-2010-responses.csv')");
		stmt.execute("CREATE TABLE GLA_SURVEY (Gender VARCHAR(255),\n" + 
				"Age VARCHAR(255),\n" + 
				"EthnicGroup VARCHAR(255),\n" + 
				"WorkingStatus VARCHAR(255),\n" + 
				"HomeDescription VARCHAR(255),\n" + 
				"CurrentAddress VARCHAR(255),\n" + 
				"HowLongInLondon VARCHAR(255),\n" + 
				"NeighborhoodSatisfaction VARCHAR(255),\n" + 
				"LondonSatisfaction VARCHAR(255),\n" + 
				"NewsSource CLOB )"
				+ " AS SELECT * FROM GLA_SURVEY_SOURCE");
		
		conn.close();
		
		logger.info("Closing the Connection");
		
		logger.info("Launching Runner");
		
		SQLExecutor.main("src/test/resources/test-sql-runner.xml");
		
		byte[] d1 = Files.readAllBytes(Paths.get("target/output-csv/gender-count.csv"));
		byte[] d2 = Files.readAllBytes(Paths.get("target/output-csv/how-long-london.csv"));
		
		logger.info("\n" + new String(d1));
		logger.info("\n" + new String(d2));
		
	}
	
	
}
