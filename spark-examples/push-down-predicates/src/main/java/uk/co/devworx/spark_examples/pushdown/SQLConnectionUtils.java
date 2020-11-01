package uk.co.devworx.spark_examples.pushdown;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * A simple somewhat trivial class that helps out with the administration of connections
 * 
 * @author jsteenkamp
 *
 */
public class SQLConnectionUtils implements Closeable
{
	public static final String DB_PATH = "target/orders_db/orders_events";

	public static final String getJDBC_URL()
	{
		try
		{
			Path p = Paths.get(DB_PATH);
			if (Files.exists(p.getParent()) == false)
			{
				Files.createDirectories(p.getParent());
			}

			return "jdbc:h2:" + p.toAbsolutePath();
		}
		catch(Exception e)
		{
			throw new RuntimeException(e);
		}
	}



	private final String driverClass; 
	private final String url; 
	private final String username; 
	private final String password;
	
	private final Connection connection;

	public static SQLConnectionUtils getDefaultInstance()
	{
		String jdbc = getJDBC_URL();
		System.out.println("JDBC URL : " +  jdbc);
		return new SQLConnectionUtils(org.h2.Driver.class.getCanonicalName(), jdbc, "sa", "");

	}

	public SQLConnectionUtils(String driverClassP,
							  String urlP,
							  String usernameP,
							  String passwordP)
	{
		this.driverClass = driverClassP;
		this.url = urlP;
		this.username = usernameP;
		this.password = passwordP;
		
		try
		{
			Class<?> forName = Class.forName(driverClass);
		} 
		catch (ClassNotFoundException e)
		{
			String msg = "Unable to load the driver manager : " + e + " -> Class was : " + driverClass;
			throw new RuntimeException(msg, e);
		}
		
		try
		{
			connection = DriverManager.getConnection(url, username, password);
		} 
		catch (SQLException e)
		{
			String msg = "Unable to load connect to the database with: " + url + " ->  " + e;
			throw new RuntimeException(msg, e);
		}

	}

	public static Properties getProperties()
	{
		Properties properties = new Properties();
		properties.put("user", "sa");
		properties.put("password", "");
		return properties;
	}

	public Connection getConnection()
	{
		return connection;
	}

	@Override
	public void close() throws IOException
	{
		try
		{
			if(connection != null) connection.close();
		} 
		catch (SQLException e)
		{
			String msg = "Unable to close the database with: " + url + " ->  " + e;
		}
		
	}
	
}
