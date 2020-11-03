package uk.co.devworx.spark.xsdschema;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The attribute path leading to a particular member.
 */
public class MemberPath implements Comparable<MemberPath>, Serializable
{
	private static final long serialVersionUID = 1L;

	public static final String ATTRIBUTE_SEPARATOR_COL_NAME = "_";

	private final Class<?> rootType;
	private final int ordinal;
	private final List<String> attributePath;
	private final DataType dataType;
	private final Class<?> valueType;
	private final String xmlFieldName;
	private final Class<?> listGenericType; //Warning can be null.... but can't make it optional here for serialization

	MemberPath(Class<?> rootType,
			   int ordinal,
			   List<String> attributePath,
			   DataType dataType,
			   Class<?> valueType,
			   String xmlFieldNameP)
	{
		this.rootType = rootType;
		this.ordinal = ordinal;
		this.attributePath = attributePath;
		this.dataType = dataType;
		this.valueType = valueType;
		this.xmlFieldName = xmlFieldNameP;
		this.listGenericType = null;
	}

	MemberPath(Class<?> rootType, int ordinal, List<String> attributePath, DataType dataType, Class<?> valueType, String xmlFieldNameP, Class<?> listGenericType)
	{
		this.rootType = rootType;
		this.ordinal = ordinal;
		this.attributePath = attributePath;
		this.dataType = dataType;
		this.valueType = valueType;
		this.xmlFieldName = xmlFieldNameP;
		this.listGenericType = listGenericType;
	}

	public String buildColumnName()
	{
		if(attributePath.size() == 1) return xmlFieldName;

		StringBuilder columnName = new StringBuilder();
		for(String s : attributePath)
		{
			columnName.append(s);
			columnName.append(ATTRIBUTE_SEPARATOR_COL_NAME);
		}
		columnName.deleteCharAt(columnName.length() - 1);
		return columnName.toString();
	}

	public StructField createStructField()
	{
		MetadataBuilder md = new MetadataBuilder();
		md.putString("rootType", rootType.getCanonicalName());
		md.putStringArray("attributePath", attributePath.toArray(new String[attributePath.size()]));
		md.putString("valueType", valueType.getCanonicalName());
		md.putString("xmlFieldName", xmlFieldName);
		if(listGenericType != null )
		{
			md.putString("listGenericType", listGenericType.getCanonicalName());
		}

		StructField field = DataTypes.createStructField(buildColumnName(), dataType, true);

		return field;
	}

	/**
	 * Creates a clone of this item - but with a new prefix and super class type.
	 */
	public MemberPath cloneWithPrefixAndRootType(final Class<?> inputClass,
												 final String variableName,
												 final int newOrdinal)
	{
		ArrayList<String> newPath = new ArrayList<>();
		newPath.add(variableName);
		newPath.addAll(attributePath);
		MemberPath clone = new MemberPath(inputClass, newOrdinal, newPath, dataType,valueType, xmlFieldName, listGenericType);
		return clone;
	}

	public Class<?> getRootType()
	{
		return rootType;
	}

	public int getOrdinal()
	{
		return ordinal;
	}

	public List<String> getAttributePath()
	{
		return attributePath;
	}

	public DataType getDataType()
	{
		return dataType;
	}

	public Class<?> getValueType()
	{
		return valueType;
	}

	public String getXmlFieldName()
	{
		return xmlFieldName;
	}

	public Optional<Class<?>> getListGenericType()
	{
		return Optional.ofNullable(listGenericType);
	}

	@Override public String toString()
	{
		return "MemberPath{" + "rootType=" + rootType + ", ordinal=" + ordinal + ", attributePath=" + attributePath + ", dataType=" + dataType + ", valueType=" + valueType + ", xmlFieldName='" + xmlFieldName + '\'' + ", listGenericType=" + listGenericType + '}';
	}

	@Override public int compareTo(MemberPath o)
	{
		if(ordinal > o.ordinal) return 1;
		if(ordinal < o.ordinal) return -1;
		return xmlFieldName.compareTo(o.xmlFieldName);
	}

	/**
	 * Extracts the value from this object - given the attribute paths.
	 *
	 * @param subject
	 * @return
	 */
	public Object extractRowValue(final XSDSparkSchemaService service, Object subject)
	{
		Objects.requireNonNull(subject, "You cannot pass in a null raw value to extract !");
		Object rawObject = extractRawObject(subject);
		if(rawObject == null)
		{
			return null;
		}

		if(service.isEnumValue(rawObject) == true)
		{
			return service.getEnumValue(rawObject);
		}

		final Class<?> rowObjectClass = rawObject.getClass();
		final Optional<MemberPaths> memberPathsOpt = service.getMemberPaths(rowObjectClass);

		if(memberPathsOpt.isPresent() == true)
		{
			final MemberPaths memberPaths = memberPathsOpt.get();
			return memberPaths.createRowArray(service, rawObject);
		}

		if(List.class.isAssignableFrom(rowObjectClass))
		{
			final List<?> rowList = (List)rawObject;
			if(service.isEnumValue(listGenericType) == true)
			{
				//An array of member types.
				String[] enumItems = new String[rowList.size()];
				for (int i = 0; i < enumItems.length; i++)
					if(rowList.get(i) != null)
						enumItems[i] = String.valueOf(rowList.get(i));
				return enumItems;
			}

			final Optional<MemberPaths> genericTypeOpt = service.getMemberPaths(listGenericType);
			if(genericTypeOpt.isPresent() == true)
			{
				final MemberPaths genericType = genericTypeOpt.get();
				final Object[] compoundValues = new Object[rowList.size()];
				for (int i = 0; i < compoundValues.length; i++)
				{
					if (rowList.get(i) != null)
					{
						Row innerRow = genericType.createRow(service, rowList.get(i));
						compoundValues[i] = innerRow;
					}
				}
				return compoundValues;
			}

			//Ok, so this must be a bunch of primitive types.
			final Object[] primValues = new Object[rowList.size()];
			for (int i = 0; i < primValues.length; i++)
			{
				if (rowList.get(i) != null)
				{
					primValues[i] = XSDSparkSchemaService.extractSparkCompatiblePrimitive(rowList.get(i));
				}
			}

			return primValues;
		}

		//Ok - so this must be a simple spark
		return XSDSparkSchemaService.extractSparkCompatiblePrimitive(rawObject);
	}

	private Object extractRawObject(final Object subjectP)
	{
		Object subject = subjectP;
		Class<?> underlyngClass = subject.getClass();

		for (int i = 0; i < attributePath.size(); i++)
		{
			final String attribute = attributePath.get(i);
			final boolean isLast = (i == attributePath.size() - 1);
			final PropertyDescriptor descriptor = getPropertyDescriptor(underlyngClass, attribute);
			final Object resultOfInvocation = invokeReadMethod(subject, descriptor);

			if(isLast == true)
			{
				return resultOfInvocation;
			}
			if(resultOfInvocation == null)
			{
				return null;
			}

			subject = resultOfInvocation;
			underlyngClass = subject.getClass();
		}

		throw new IllegalArgumentException("This line should never be reached - we have an explicit return in the loop");
	}

	private Object invokeReadMethod(final Object subject, PropertyDescriptor descriptor)
	{
		final Method readMethod = descriptor.getReadMethod();
		try
		{
			return readMethod.invoke(subject);
		}
		catch(Exception e)
		{
			throw new RuntimeException("Unable to invoke the property descriptor's read method of - " + readMethod + " - got the exception : " + e);
		}
	}

	public PropertyDescriptor getPropertyDescriptor(Class<?> underlyngClass, String attributeName)
	{
		final BeanInfo beanInfo = getBeanInfo(underlyngClass);
		PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
		for (int i = 0; i < propertyDescriptors.length; i++)
		{
			if(propertyDescriptors[i].getName().equals(attributeName))
			{
				return propertyDescriptors[i];
			}
		}

		throw new IllegalArgumentException("Could not find a property name : " + attributeName + " in the class " + underlyngClass);
	}

	public BeanInfo getBeanInfo(Class<?> underlyngClass)
	{
		try
		{
			BeanInfo beanInfo = Introspector.getBeanInfo(underlyngClass);
			return beanInfo;
		}
		catch(Exception e)
		{
			throw new RuntimeException("Could not get the bean info for : " + underlyngClass + " - " + e);
		}

	}

}
