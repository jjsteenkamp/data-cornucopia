package uk.co.devworx.spark_examples.pushdown;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class OrderEvent
{
	private final String _entityId;
	private final BigDecimal customer_order_base_price;
	private final String event_type;
	private final String status;
	private final LocalDateTime dateTime;

	public OrderEvent(String _entityId, BigDecimal customer_order_base_price, String event_type, String status, LocalDateTime dateTime)
	{
		this._entityId = _entityId;
		this.customer_order_base_price = customer_order_base_price;
		this.event_type = event_type;
		this.status = status;
		this.dateTime = dateTime;
	}

	public String get_entityId()
	{
		return _entityId;
	}

	public BigDecimal getCustomer_order_base_price()
	{
		return customer_order_base_price;
	}

	public String getEvent_type()
	{
		return event_type;
	}

	public String getStatus()
	{
		return status;
	}

	public LocalDateTime getDateTime()
	{
		return dateTime;
	}
}
