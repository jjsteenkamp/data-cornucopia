package uk.co.devworx.etcetera;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.Arrays;

/**
 * A simple test case to see if the '==' operator
 * works for class that has been obtained via the Java Bean mechanisms.
 *
 * @author jsteenkamp
 *
 */
public class ClassReferenceEqualsTest
{

	public static class SimpleBean
	{
		public Boolean getMyValue()
		{
			return myValue;
		}

		public void setMyValue(Boolean myValue)
		{
			this.myValue = myValue;
		}

		Boolean myValue;
	}


	@Test
	public void testClassReferenceEquals() throws Exception
	{
		BeanInfo beanInfo = Introspector.getBeanInfo(SimpleBean.class);
		PropertyDescriptor myValueDescr = Arrays.stream(beanInfo.getPropertyDescriptors()).filter(p -> p.getName().equals("myValue")).findAny().get();

		System.out.println("Read  method : " + myValueDescr.getReadMethod());
		System.out.println("Write method : " + myValueDescr.getWriteMethod());

		Class<?> propertyType = myValueDescr.getPropertyType();

		System.out.println("Property Type Equals : " + (Boolean.class.equals(propertyType)) );
		System.out.println("Property Type == : " + (Boolean.class == propertyType) );

		Assertions.assertEquals(true, (Boolean.class.equals(propertyType)));
		Assertions.assertEquals(true, (Boolean.class == propertyType));

	}




}



