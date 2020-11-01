package uk.co.devworx.spark_examples.pushdown;

public class MyLocalTime extends java.sql.Date
{
	public MyLocalTime(int year, int month, int day)
	{
		super(year, month, day);
	}

	public MyLocalTime(long date)
	{
		super(date);
	}

	@Override public int getHours()
	{
		return 10;
	}

	@Override public int getMinutes()
	{
		return 10;
	}

	@Override public int getSeconds()
	{
		return 10;
	}

	@Override public void setHours(int i)
	{
		super.setHours(i);
	}

	@Override public void setMinutes(int i)
	{
		super.setMinutes(i);
	}

	@Override public void setSeconds(int i)
	{
		super.setSeconds(i);
	}
}
