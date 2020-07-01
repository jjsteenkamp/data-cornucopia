package uk.co.devworx.jdbc.incrementer;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;

/**
 * Some result set utilities that may on occasion be useful for debugging purposes. 
 * 
 *
 *
 */
public class ResultSetUtils
{
	private ResultSetUtils() {} //Never instantiate
	
	
	/**
	 * Get all the column names 
	 * 
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	public static List<String> getColumnNames(ResultSet rs) throws SQLException
	{
		ResultSetMetaData md = rs.getMetaData();
		List<String> res = new ArrayList<>();
		int cols = md.getColumnCount();
		for (int i = 1; i <= cols; i++)
		{
			res.add(md.getColumnName(i));
		}
		return Collections.unmodifiableList(res);
	}
	
	
	/**
	 * A generic function for turning a ResultSet into a nicely formatted String
	 * 
	 * @param rs
	 * @return
	 */
	public static StringBuilder toString(ResultSet rs) throws SQLException
	{
		final int rowToResetTo = rs.getRow();
		final StringBuilder output = new StringBuilder();
		
		if(rs.getType() != ResultSet.TYPE_FORWARD_ONLY) rs.beforeFirst();
		
		output.append("\n" );

		final List<String> columnNames = getColumnNames(rs);
		AsciiTable at = new AsciiTable();
		at.addRule();
		at.addRow(columnNames);
		at.addRule();

		while(rs.next())
		{
			final Object[] row = new String[columnNames.size()];	
			for (int i = 0; i < row.length; i++)
			{
				Object obj = rs.getObject(i + 1);
				row[i] = String.valueOf(obj);
			}
			at.addRow(row);
		}
		at.addRule();
		
		CWC_LongestLine cwc = new CWC_LongestLine();
		at.getRenderer().setCWC(cwc);
		output.append(at.render());
		
		if(rs.getType() != ResultSet.TYPE_FORWARD_ONLY)
		{
			if(rowToResetTo == 0)
			{
				rs.beforeFirst();
			}
			else
			{
				rs.absolute(rowToResetTo);
			}
		}
		
		return output;
	}
	

}
