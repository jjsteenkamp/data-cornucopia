package uk.co.devworx.spark.xsdschema;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The standalone spark schema service class that
 * will contain the definitions and lookups for all the member items
 */
public class XSDSparkSchemaService implements Serializable
{
	private static final long serialVersionUID = 1L;

	public static final String EMBEDDED_SCHEMA_SERVICE = "spark-schema/XSDSparkSchemaService.bin";

	/**
	 * Gets the instance of this service that was loaded from the classpath. This is sufficient for most users.
	 */
	public static XSDSparkSchemaService getInstance()
	{
		return ___getInstance();
	}

	public static final int BIG_DECIMAL_SCALE = 10;
	public static final int BIG_DECIMAL_PRECISION = 38;

	private final Map<Class<?>, MemberPaths> classMemberPaths;
	private final Set<Class<?>> serviceEnumTypes;
	private final Map<Class<?>, MemberPathsRowReader> rowReaderMap;

	XSDSparkSchemaService(Map<Class<?>, MemberPaths> classMemberPaths,
						 Set<Class<?>> serviceEnumTypesP)
	{
		this.classMemberPaths = classMemberPaths;
		this.serviceEnumTypes = serviceEnumTypesP;
		this.rowReaderMap = new ConcurrentHashMap<>();
		classMemberPaths.forEach((k,v) ->
		{
			rowReaderMap.put(k, new MemberPathsRowReader(v, XSDSparkSchemaService.this));
		});
	}

	public Optional<MemberPaths> getMemberPaths(Class<?> classItem)
	{
		MemberPaths memberPaths = classMemberPaths.get(classItem);
		return Optional.ofNullable(memberPaths);
	}

	public <T> Optional<MemberPathsRowReader<T>> getRowReader(Class<T> classItem)
	{
		MemberPathsRowReader<T> rowReader = rowReaderMap.get(classItem);
		return Optional.ofNullable(rowReader);
	}


	public Row createRow(Object subject)
	{
		Objects.requireNonNull(subject, "You cannot pass in a null argument to this function ! ");
		Class<?> inputClass = subject.getClass();
		Optional<MemberPaths> memberPathsOpt = getMemberPaths(inputClass);

		if(memberPathsOpt.isPresent() == false)
		{
			throw new IllegalArgumentException("Could not find the input class : " + inputClass + " cannot flatten your object !");
		}

		MemberPaths memberPaths = memberPathsOpt.get();
		Row row = memberPaths.createRow(this, subject);
		return row;
	}

	public boolean isEnumValue(Object subject)
	{
		Objects.requireNonNull(subject, "You cannot pass in a null argument to this function ! ");
		final Class<?> inputClass = subject.getClass();
		return serviceEnumTypes.contains(inputClass);
	}

	public Object getEnumValue(Object subject)
	{
		Objects.requireNonNull(subject, "You cannot pass in a null argument to this function ! ");
		if(isEnumValue(subject) == false)
		{
			throw new IllegalArgumentException("Cannot get a value from a type that is not an enum value - subject is : " + subject + " | " + subject.getClass() + " \n " +  serviceEnumTypes);
		}
		return subject.toString();
	}

	public static Object convertEnumStringToValue(Class<?> enumClass, String enumValue)
	{
		return Enum.valueOf((Class)enumClass, enumValue);
	}

	public static Object convertToJAXBCompatiblePrimitive(final Class<?> valueType, final Object sparkInput)
	{
		Objects.requireNonNull(sparkInput, "You cannot pass in a null argument to this function ! ");
		Class<?> referencedClass = sparkInput.getClass();

		if(valueType.equals(BigInteger.class))
		{
			return BigInteger.valueOf( ((Number)sparkInput).longValue() );
		}

		if(	 	referencedClass.equals(String.class) ||
				referencedClass.equals(LocalDate.class) ||
				referencedClass.equals(BigDecimal.class) ||
				referencedClass.equals(Boolean.class))
		{
			return sparkInput;
		}

		if(referencedClass.equals(BigInteger.class))
		{
			BigInteger bi = (BigInteger)sparkInput;
			return bi.longValue();
		}

		if(referencedClass.equals(Timestamp.class))
		{
			final Timestamp timestamp = (Timestamp)sparkInput;
			if( valueType.equals(LocalDateTime.class) )
			{
				return timestamp.toLocalDateTime();
			}
			else if( valueType.equals(LocalDate.class) )
			{
				return timestamp.toLocalDateTime().toLocalDate();
			}
			else
			{
				throw new RuntimeException("Could not convert value - " + sparkInput + " - class of type : " + referencedClass + " - to output of : " + valueType);
			}
		}

		throw new RuntimeException("Unable to convert the spark type ("  + sparkInput  + " of class " + referencedClass + "  ) to : " + valueType);
	}

	public static Object extractSparkCompatiblePrimitive(Object rawValue)
	{
		Objects.requireNonNull(rawValue, "You cannot pass in a null argument to this function ! ");
		Class<?> referencedClass = rawValue.getClass();

		if(	 	referencedClass.equals(String.class) ||
				referencedClass.equals(Long.class) ||
				referencedClass.equals(BigDecimal.class) ||
				referencedClass.equals(Boolean.class))
		{
			return rawValue;
		}

		if(referencedClass.equals(BigInteger.class))
		{
			BigInteger bi = (BigInteger)rawValue;
			return bi.longValue();
		}

		if(referencedClass.equals(LocalDateTime.class))
		{
			LocalDateTime localDateTime = (LocalDateTime)(rawValue);
			return Timestamp.valueOf(localDateTime);
		}
		if(referencedClass.equals(LocalDate.class))
		{
			LocalDate localDate = (LocalDate)(rawValue);
			return localDate;//Timestamp.valueOf(localDate.atStartOfDay());
		}
		if(referencedClass.equals(LocalTime.class))
		{
			LocalTime localTime = (LocalTime)(rawValue);
			return String.valueOf(localTime);
		}

		throw new RuntimeException("Unable to determine the spark type from : " + referencedClass);
	}

	public static DataType determineSparkDataTypeFromLeafType(Class<?> referencedClass)
	{
		if(referencedClass.equals(String.class))
		{
			return DataTypes.StringType;
		}
		if(referencedClass.equals(BigInteger.class) || referencedClass.equals(Long.class))
		{
			return DataTypes.LongType;
		}
		if(referencedClass.equals(Boolean.class))
		{
			return DataTypes.BooleanType;
		}
		if(referencedClass.equals(BigDecimal.class))
		{
			return DataTypes.createDecimalType(BIG_DECIMAL_PRECISION,BIG_DECIMAL_SCALE);
		}
		if(referencedClass.equals(LocalDateTime.class))
		{
			return DataTypes.TimestampType;
		}
		if(referencedClass.equals(LocalDate.class))
		{
			return DataTypes.DateType;
		}
		if(referencedClass.equals(LocalTime.class))
		{
			return DataTypes.StringType;
		}

		throw new RuntimeException("Unable to determine the spark type from : " + referencedClass);
	}

	public static Class<?> determineFullClass(Class<?> inputType, String name)
	{
		if(name.equals("String")) return String.class;
		if(name.equals("BigInteger")) return BigInteger.class;
		if(name.equals("BigDecimal")) return BigDecimal.class;
		if(name.equals("Long")) return Long.class;
		if(name.equals("LocalDateTime")) return LocalDateTime.class;
		if(name.equals("LocalDate")) return LocalDate.class;
		if(name.equals("LocalTime")) return LocalTime.class;
		if(name.equals("Boolean")) return Boolean.class;
		if(name.equals("List")) return List.class;

		String fullName =  inputType.getPackage().getName() + "." + name;
		try
		{
			return Class.forName(fullName);
		}
		catch(Exception e)
		{
			throw new RuntimeException("Unable to parse the class name : " + fullName + " - you have to check the logic for the type : " + inputType, e);
		}

	}

	public Map<Class<?>, MemberPaths> getClassMemberPaths()
	{
		return Collections.unmodifiableMap(classMemberPaths);
	}


	private static final AtomicReference<XSDSparkSchemaService> embeddedInstance = new AtomicReference<>();
	private static XSDSparkSchemaService ___getInstance()
	{
		XSDSparkSchemaService xsdSparkSchemaService = embeddedInstance.get();
		if(xsdSparkSchemaService != null) return xsdSparkSchemaService;

		synchronized (embeddedInstance)
		{
			xsdSparkSchemaService = embeddedInstance.get();
			if(xsdSparkSchemaService != null) return xsdSparkSchemaService;

			final ClassLoader loader = XSDSparkSchemaService.class.getClassLoader();
			final String attempt1 = EMBEDDED_SCHEMA_SERVICE;
			final String attempt2 = "/" + EMBEDDED_SCHEMA_SERVICE;
			InputStream ins = loader.getResourceAsStream(attempt1);
			if(ins == null)
			{
				ins = loader.getResourceAsStream(attempt2);
			}
			if(ins == null)
			{
				throw new RuntimeException("Unable to load the embedded spark schema file - either using : " + attempt1 + " or " + attempt2 );
			}
			try(ObjectInputStream ous = new ObjectInputStream(ins))
			{
				xsdSparkSchemaService = (XSDSparkSchemaService)ous.readObject();
				embeddedInstance.set(xsdSparkSchemaService);
				return xsdSparkSchemaService;
			}
			catch(Exception e)
			{
				throw new RuntimeException("Unable to read the embedded data file : " + EMBEDDED_SCHEMA_SERVICE + " - got the exception : " + e);
			}
		}
	}

}
