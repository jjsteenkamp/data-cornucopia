package uk.co.devworx.spark_examples.elt.model;

import java.util.Collection;


/**
 * The generic description for a build validation class
 * 
 * @author jsteenkamp
 *
 */
public class BuildValidationFailure
{
	private final String description;
	private final Class<?> targetClass;
	private final String fieldName;
	private final String valueSupplied;
	
	public static BuildValidationFailure newFailure( Class<?> targetClass, String fieldName, String valueSupplied, String description )
	{
		return new BuildValidationFailure(description, targetClass, fieldName, valueSupplied);
	}
	
	private BuildValidationFailure(String description, Class<?> targetClass, String fieldName, String valueSupplied)
	{
		super();
		this.description = description;
		this.targetClass = targetClass;
		this.fieldName = fieldName;
		this.valueSupplied = valueSupplied;
	}
	
	public static String summaryReport(Collection<BuildValidationFailure> failures)
	{
		StringBuilder bldr = new StringBuilder();
		bldr.append("Target Class,Field Name,Value Supplied,Description");
		bldr.append("\n");
		failures.forEach(f -> 
		{
			bldr.append(String.valueOf(f.getTargetClass()));
			bldr.append(",");
			bldr.append(String.valueOf(f.getFieldName()));
			bldr.append(",");
			bldr.append(String.valueOf(f.getValueSupplied()));
			bldr.append(",");
			bldr.append(String.valueOf(f.getDescription()));
			bldr.append("\n");
		});
		
		return bldr.toString();
	}
	

	public String getDescription()
	{
		return description;
	}

	public Class<?> getTargetClass()
	{
		return targetClass;
	}

	public String getFieldName()
	{
		return fieldName;
	}

	public String getValueSupplied()
	{
		return valueSupplied;
	}

	@Override
	public String toString()
	{
		return "BuildValidationFailure [description=" + description + ", targetClass=" + targetClass + ", fieldName=" + fieldName + ", valueSupplied=" + valueSupplied + "]";
	}
	
		
}
