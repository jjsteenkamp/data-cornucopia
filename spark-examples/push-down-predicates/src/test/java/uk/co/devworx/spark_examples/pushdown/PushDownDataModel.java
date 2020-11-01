package uk.co.devworx.spark_examples.pushdown;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

/**
 * An assorted set of values on the data model
 */
public class PushDownDataModel
{
	private Long aInteger;
	private BigDecimal aBigDecimal;
	private LocalDate aLocalDate;
	private Instant aInstant;
	private MyLocalTime aLocalTime;

	public static PushDownDataModel getRandom()
	{
		PushDownDataModel model = new PushDownDataModel();
		model.setaInstant(Instant.now());
		model.setaInteger((1090L));
		model.setaBigDecimal(BigDecimal.valueOf(100));
		model.setaLocalTime(new MyLocalTime(System.currentTimeMillis()));
		model.setaLocalDate(LocalDate.now());
		return model;
	}

	public BigDecimal getaBigDecimal()
	{
		return aBigDecimal;
	}

	public Long getaInteger()
	{
		return aInteger;
	}

	public void setaInteger(Long aInteger)
	{
		this.aInteger = aInteger;
	}

	public LocalDate getaLocalDate()
	{
		return aLocalDate;
	}

	public void setaLocalDate(LocalDate aLocalDate)
	{
		this.aLocalDate = aLocalDate;
	}

	public void setaBigDecimal(BigDecimal aBigDecimal)
	{
		this.aBigDecimal = aBigDecimal;
	}

	public Instant getaInstant()
	{
		return aInstant;
	}

	public void setaInstant(Instant aInstant)
	{
		this.aInstant = aInstant;
	}

	public MyLocalTime getaLocalTime()
	{
		return aLocalTime;
	}

	public void setaLocalTime(MyLocalTime aLocalTime)
	{
		this.aLocalTime = aLocalTime;
	}

}

