package uk.co.devworx.spark.xsdschema;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This object is responsible for constructing an object from a
 * row that has been previously encoded using a MemberPaths object
 *
 */
public class MemberPathsRowReader<T> implements Serializable
{
	private static final Logger logger = LogManager.getLogger(MemberPathsRowReader.class);

	private final MemberPaths memberPaths;
	private final XSDSparkSchemaService schemaService;

	MemberPathsRowReader(MemberPaths memberPaths,
						 XSDSparkSchemaService schemaService)
	{
		this.memberPaths = memberPaths;
		this.schemaService = schemaService;
	}

	public T read(Row row)
	{
		final StructType structType = memberPaths.getStructType();
		final Class<?> rootClass = memberPaths.getRootClass();
		final List<MemberPath> memberPaths = this.memberPaths.getMemberPaths();
		final T rootClassInstance = instantiateClass(rootClass);

		//Loop over the fields and recursively build up the object
		for (int i = 0; i < memberPaths.size(); i++)
		{
			final MemberPath mp = memberPaths.get(i);
			final Object rawRowValue = row.get(i);

			if(rawRowValue == null) continue;

			setMemberPathIndividualValueOnObject(rawRowValue, schemaService, mp, rootClass, rootClassInstance);

		}

		return rootClassInstance;
	}

	private static <T> void setMemberPathIndividualValueOnObject(final Object rawRowValue,
														  		 final XSDSparkSchemaService schemaService,
														  		 final MemberPath mp,
																 final Class<?> rootClass,
																 final T rootClassInstance
														  		 )
	{
		final List<String> attributePaths = mp.getAttributePath();
		final String firstAttribute = attributePaths.get(0);
		final PropertyDescriptor property = mp.getPropertyDescriptor(rootClass, firstAttribute);
		final Method readMethod = property.getReadMethod();
		final Method writeMethod = property.getWriteMethod();
		final Class<?> returnType = readMethod.getReturnType();

		if(attributePaths.size() == 1)
		{
			final boolean isResultAList = List.class.isAssignableFrom(returnType);
			if(isResultAList == true)
			{
				final ArrayList listToAddValues = (ArrayList)invokeReadMethod(readMethod, rootClassInstance); //The list gets init by jaxb
				final Class<?> listElementType = mp.getListGenericType().orElseThrow(() -> new RuntimeException("Expected - we need a list generic type for : " + mp));
				Object[] rawRowArray = (Object[])rawRowValue;
				if(rawRowArray.length == 0)
				{
					return; //Nothing to do here.
				}

				for (int i = 0; i < rawRowArray.length; i++)
				{
					Object rawRowArrayElement = rawRowArray[i];
					if(rawRowArrayElement instanceof Row)
					{
						MemberPathsRowReader<?> subnElementReader = schemaService.getRowReader(listElementType).orElseThrow(() -> new RuntimeException("Expected - we need a list reader for for : " + listElementType));
						Object instanceRead = subnElementReader.read((Row) rawRowArrayElement);
						listToAddValues.add(instanceRead);
					}
					else if(listElementType.isEnum() == true)
					{
						listToAddValues.add(XSDSparkSchemaService.convertEnumStringToValue(listElementType, String.valueOf(rawRowArrayElement) ));
					}
					else
					{
						Object convertedValue = XSDSparkSchemaService.convertToJAXBCompatiblePrimitive(listElementType, rawRowArrayElement);
						listToAddValues.add(convertedValue);
					}
				}
			}
			else if(returnType.isEnum() == true)
			{
				Object convertedValue = XSDSparkSchemaService.convertEnumStringToValue(returnType, String.valueOf(rawRowValue));
				invokeWriteMethod(writeMethod,rootClassInstance, convertedValue);
			}
			else
			{
				//Simple primitive setters.
				Object convertedValue = XSDSparkSchemaService.convertToJAXBCompatiblePrimitive(returnType, rawRowValue);
				invokeWriteMethod(writeMethod,rootClassInstance, convertedValue);
			}
		}
		else
		{
			//We need to be able to find this item.
			final Optional<MemberPaths> memberPathsOpt = schemaService.getMemberPaths(returnType);
			if(memberPathsOpt.isPresent() == false)
			{
				throw new RuntimeException("Unable to find a schema converter for : " + returnType + " - this was for attribute - " + firstAttribute + " - MemberPath : " + mp);
			}

			final MemberPaths memberPaths = memberPathsOpt.get();
			Object subjectObject = invokeReadMethod(readMethod,rootClassInstance);
			if(subjectObject == null)
			{
				subjectObject = instantiateClass(returnType);
				invokeWriteMethod(writeMethod,rootClassInstance,subjectObject);
			}
			//Recurse on this object.
			final MemberPath inner_mp = memberPaths.findMatchingLevelDownMemberPath(mp);
			setMemberPathIndividualValueOnObject(rawRowValue,
												 schemaService,
												 inner_mp,
												 inner_mp.getRootType(),
												 subjectObject);
		}

	}

	private static void invokeWriteMethod(Method writeMethod, Object instance, Object valueToSet)
	{
		try
		{
			writeMethod.invoke(instance, valueToSet);
		}
		catch(Exception e)
		{
			String msg = "Unable to invoke the write method " + writeMethod + " - encountered the exception : " + e + "\n" +
						 "Value to set : " + valueToSet + " (type : " + valueToSet.getClass() +") \n " +
						 "Argument : " + writeMethod.getParameterTypes()[0] + "\n";
			logger.error(msg, e);
			throw new RuntimeException(msg, e);
		}
	}

	private static <T> Object invokeReadMethod(Method readMethod, T rootClassInstance)
	{
		try
		{
			final Object result = readMethod.invoke(rootClassInstance);
			return result;
		}
		catch(Exception e)
		{
			String msg = "Unable to invoke the read method " + readMethod + " - encountered the exception : " + e;
			logger.error(msg, e);
			throw new RuntimeException(msg, e);
		}
	}

	private static <T> T instantiateClass(Class<?> rootClass)
	{
		try
		{
			final Constructor<?> defaultConstructor = rootClass.getConstructor();
			return (T)defaultConstructor.newInstance();
		}
		catch(Exception e)
		{
			String msg = "Unable to instatiate the root class : " + rootClass + " - encountered the exception : " + e;
			logger.error(msg, e);
			throw new RuntimeException(msg, e);
		}

	}

}
