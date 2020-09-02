package uk.co.devworx.spark_examples.pushdown;

import java.math.BigDecimal;

public class Order
{
	private final String _entityId;
	private final String entity_time;
	private final String order_type;
	private final String instrument;
	private final String direction;
	private final String state;
	private final String status;
	private final BigDecimal last_confirmed_amount;
	private final BigDecimal filled_amount;

	public Order(String _entityId, String entity_time, String order_type, String instrument, String direction, String state, String status, BigDecimal last_confirmed_amount, BigDecimal filled_amount)
	{
		this._entityId = _entityId;
		this.entity_time = entity_time;
		this.order_type = order_type;
		this.instrument = instrument;
		this.direction = direction;
		this.state = state;
		this.status = status;
		this.last_confirmed_amount = last_confirmed_amount;
		this.filled_amount = filled_amount;
	}

	public String get_entityId()
	{
		return _entityId;
	}

	public String getEntity_time()
	{
		return entity_time;
	}

	public String getOrder_type()
	{
		return order_type;
	}

	public String getInstrument()
	{
		return instrument;
	}

	public String getDirection()
	{
		return direction;
	}

	public String getState()
	{
		return state;
	}

	public String getStatus()
	{
		return status;
	}

	public BigDecimal getLast_confirmed_amount()
	{
		return last_confirmed_amount;
	}

	public BigDecimal getFilled_amount()
	{
		return filled_amount;
	}
}
