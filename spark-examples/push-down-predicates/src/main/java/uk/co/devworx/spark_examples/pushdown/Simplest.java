package uk.co.devworx.spark_examples.pushdown;

import java.util.Optional;

public class Simplest
{
	private String name;
	private double value;
	private Simplest_Inner inner;
	private Optional<String> additionalName;

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public double getValue()
	{
		return value;
	}

	public void setValue(double value)
	{
		this.value = value;
	}

	public Simplest_Inner getInner()
	{
		return inner;
	}

	public void setInner(Simplest_Inner inner)
	{
		this.inner = inner;
	}

	public Optional<String> getAdditionalName()
	{
		return additionalName;
	}

	public void setAdditionalName(Optional<String> additionalName)
	{
		this.additionalName = additionalName;
	}
}
