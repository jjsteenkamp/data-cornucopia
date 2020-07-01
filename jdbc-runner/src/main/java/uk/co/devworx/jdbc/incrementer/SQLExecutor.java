package uk.co.devworx.jdbc.incrementer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SQLExecutor
{
	private static final Logger logger = LogManager.getLogger(SQLExecutor.class);
	
	public static final boolean RUN_PARALLEL;
	static
	{
		String parallel = System.getProperty("SQLExecutor.RunParallel","false");
		RUN_PARALLEL = Boolean.parseBoolean(parallel);
	}
	
	public static void main(String... args) throws Exception 
	{
		if(args.length < 1)
		{
			throw new RuntimeException("You need to specify 1 parameter ! - the location of the xml file");
		}
		
		Path inputXml = Paths.get(args[0]);
		run(inputXml);
	}
	
	public static void run(Path xmlInputFile)
	{
		if(Files.exists(xmlInputFile) == false || Files.isReadable(xmlInputFile) == false)
		{
			throw new RuntimeException("The file you have specified : " + xmlInputFile.toAbsolutePath()+ " - does not exists.");
		}
		
		try(InputStream bufs = Files.newInputStream(xmlInputFile))
		{
			final JAXBContext jaxbContext = JAXBContext.newInstance(SQLRunners.class);
			final Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
			
			final SQLRunners root = (SQLRunners)unmarshaller.unmarshal(bufs);
			List<SQLRunner> runners = root.runners;

			logger.info("Doing a run of " + runners.size() + " SQLs !");

			if(RUN_PARALLEL == true)
			{
				logger.info("Executing the Runners using Parallel Execution ");
				runners.stream().parallel().forEach(SQLExecutor::executeRunner);
			}
			else
			{
				logger.info("Executing the Runners using Serial Execution ");
				runners.stream().forEach(SQLExecutor::executeRunner);
			}
		}
		catch(Exception e)
		{
			logger.error("Unable to execute the SQL : " + e);
			throw new RuntimeException(e);
		}
		logger.info("ALL DONE !");
	}
	
	public static void executeRunner(final SQLRunner runner)
	{
		try
		{
			Class.forName(runner.jdbcDriver);
		} 
		catch (Exception e)
		{
			throw new RuntimeException("Could not load your JDBC Driver : " + runner.jdbcDriver + " - " + e, e);
		}
		
		Connection connection = null;
		Statement stmtPre = null;
		
		try
		{
			try
			{
				logger.info(Instant.now() + " ->  Attempting to make the connection to the JDBC Database : " + runner.jdbcUrl);
				connection = DriverManager.getConnection(runner.jdbcUrl, runner.jdbcUsername, runner.jdbcPassword);
				logger.info(Instant.now() + " ->  Successfully made the connection to the JDBC Database : " + runner.jdbcUrl);
				stmtPre = connection.createStatement();
			} 
			catch (SQLException e)
			{
				throw new RuntimeException("Could not connect to your datanase : " + runner.jdbcUrl + " , "  + 
											runner.jdbcUsername + " , "  + runner.jdbcPassword + " - " + e, e);
			}
			
			final Statement stmt = stmtPre;
			
			runner.sqls.forEach(sql -> 
			{
				final String sqlToExecute = performSQLReplacements(runner, sql);
				final Path csvFile = Paths.get(sql.csvFile);
			
				try
				{
					if(Files.exists(csvFile.getParent()) == false)
					{
						Files.createDirectories(csvFile.getParent());
					}
				} 
				catch (IOException e1)
				{
					throw new RuntimeException("Unable to create the parent directory for the file : " + csvFile + " | " + e1, e1);
				}
				
				final boolean appendFile = sql.append == null ? false : sql.append;
				final boolean fileAlreadyExisted = csvFile.toFile().exists();
				
				try
				(   BufferedWriter bufWriter = (appendFile == true && fileAlreadyExisted == true) ? Files.newBufferedWriter(csvFile, StandardOpenOption.APPEND) : Files.newBufferedWriter(csvFile);
					CSVPrinter writer = new CSVPrinter(bufWriter, CSVFormat.EXCEL); )
				{
					logger.info("---------------------------");
					logger.info("About to execute the SQL below and place it into the file : " + sql.csvFile);
					logger.info(sqlToExecute);
					logger.info("---------------------------");
					
					ResultSet rs = stmt.executeQuery(sqlToExecute);
					ResultSetMetaData md = rs.getMetaData();
					
					int colCount =  md.getColumnCount();
					
					if(!(fileAlreadyExisted == true && appendFile == true))
					{
						//Do the header
						List<String> headerCols = new ArrayList<>();
						for (int i = 0; i < colCount; i++)
						{
							headerCols.add(md.getColumnName((i + 1)));
						}
						writer.printRecord(headerCols);
					}
					
					//Now do the data.
					while(rs.next())
					{
						List<String> row = new ArrayList<>();
						
						for (int i = 0; i < colCount; i++)
						{
							Object obj = rs.getObject(i + 1);
							if(obj == null)
							{
								row.add("");
							}
							else
							{
								if(obj instanceof java.sql.Clob)
								{
									java.sql.Clob clob = (java.sql.Clob)obj;
									try(Reader charStream = clob.getCharacterStream();
									    BufferedReader bufRader = new BufferedReader(charStream))
									{
										StringBuilder bldr = new StringBuilder();
										String line = null;
										while( (line = bufRader.readLine()) != null )
										{
											bldr.append(line);
										}
										row.add(String.valueOf(bldr.toString()));
									}
								}
								else
								{
									row.add(String.valueOf(obj));
								}
								
							}
						}
						writer.printRecord(row);
					}
					rs.close();
				}
				catch(Exception e)
				{
					logger.error("---------------------------");
					logger.error("ERROR on SQL : " + e, e);
					logger.error(sqlToExecute);
					logger.error("---------------------------");
				}
			});
		}
		finally
		{
			if(stmtPre != null)
			{
				try
				{
					stmtPre.close();
				} 
				catch (SQLException e)
				{
					logger.info("Trying to close the statement, but it didn't work - error is unactionable, so ignoring : " + e);
				}
			}
			if(connection != null)
			{
				try
				{
					connection.close();
				} 
				catch (SQLException e)
				{
					logger.info("Trying to close the connection, but it didn't work - error is unactionable, so ignoring : " + e);
				}
			}
		}
		
	}
	
	private static String performSQLReplacements(SQLRunner parentRunner, SQLScript sqlScript)
	{
		String value = sqlScript.value;
		if(parentRunner.replaceParameter == null || parentRunner.replaceParameter.isEmpty())
		{
			return value;
		}
		
		for (ReplaceParameter replParam : parentRunner.replaceParameter)
		{
			String rplVal = replParam.value;
			if(rplVal == null) rplVal = "";
			rplVal = rplVal.trim();
			value = value.replace((CharSequence)replParam.key, rplVal);
		}
	
		return value;
		
	}
	

}

























