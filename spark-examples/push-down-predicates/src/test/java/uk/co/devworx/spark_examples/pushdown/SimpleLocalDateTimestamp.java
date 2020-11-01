package uk.co.devworx.spark_examples.pushdown;

import java.sql.Timestamp;

public class SimpleLocalDateTimestamp
{
	private Timestamp eventDate;
	private String eventName;

	public Timestamp getEventDate()
	{
		return eventDate;
	}

	public void setEventDate(Timestamp eventDate)
	{
		this.eventDate = eventDate;
	}

	public String getEventName()
	{
		return eventName;
	}

	public void setEventName(String eventName)
	{
		this.eventName = eventName;
	}
}
