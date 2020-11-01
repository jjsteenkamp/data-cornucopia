package uk.co.devworx.spark_examples.pushdown;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * A simple order result
 */
public class OrderResult implements Serializable
{

	private   BigDecimal result;
	private   String entityId;
	private   OrderResultInput input;


	public BigDecimal getResult()
	{
		return result;
	}

	public OrderResultInput getInput()
	{
		return input;
	}

	public String getEntityId()
	{
		return entityId;
	}

	public void setResult(BigDecimal result)
	{
		this.result = result;
	}

	public void setEntityId(String entityId)
	{
		this.entityId = entityId;
	}

	public void setInput(OrderResultInput input)
	{
		this.input = input;
	}
}
