package uk.co.devworx.spark_examples.pushdown;

import java.io.Serializable;
import java.math.BigDecimal;

public class OrderResultInput implements Serializable
{
	private BigDecimal last_confirmed_amount;
	private BigDecimal filled_amount;
	private BigDecimal customer_order_base_price;

	public void setLast_confirmed_amount(BigDecimal last_confirmed_amount)
	{
		this.last_confirmed_amount = last_confirmed_amount;
	}

	public void setFilled_amount(BigDecimal filled_amount)
	{
		this.filled_amount = filled_amount;
	}

	public void setCustomer_order_base_price(BigDecimal customer_order_base_price)
	{
		this.customer_order_base_price = customer_order_base_price;
	}

	public BigDecimal getLast_confirmed_amount()
	{
		return last_confirmed_amount;
	}

	public BigDecimal getFilled_amount()
	{
		return filled_amount;
	}

	public BigDecimal getCustomer_order_base_price()
	{
		return customer_order_base_price;
	}
}
