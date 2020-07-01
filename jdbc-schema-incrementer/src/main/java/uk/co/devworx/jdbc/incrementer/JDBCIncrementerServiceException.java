package uk.co.devworx.jdbc.incrementer;

/**
 * A general exception for items that can go wrong on the JDBCInecrementerService service
 */
public class JDBCIncrementerServiceException extends RuntimeException
{

	public JDBCIncrementerServiceException(String message)
	{
		super(message);
	}

	public JDBCIncrementerServiceException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public JDBCIncrementerServiceException(Throwable cause)
	{
		super(cause);
	}
}
