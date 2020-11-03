package uk.co.devworx.spark.xsdschema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Objects;

/**
 * Tests the embedded loading of this service.
 */
public class XSDSparkSchemaServiceTest
{
	@Test
	public void testLoadEmbeddedService() throws Exception
	{
		XSDSparkSchemaService service = XSDSparkSchemaService.getInstance();
		XSDSparkSchemaService service2 = XSDSparkSchemaService.getInstance();

		Objects.nonNull(service);
		Objects.nonNull(service2);

		Assertions.assertTrue(service == service2);

	}


}
