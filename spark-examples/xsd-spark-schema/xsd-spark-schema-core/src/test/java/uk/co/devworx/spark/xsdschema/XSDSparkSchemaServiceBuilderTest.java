package uk.co.devworx.spark.xsdschema;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

public class XSDSparkSchemaServiceBuilderTest
{
	private static final Logger logger = LogManager.getLogger(XSDSparkSchemaServiceBuilderTest.class);
	private final Path jaxbSources = Paths.get("../xsd-spark-schema-gen/target/generated-sources/jaxb");
	private final XSDSparkSchemaServiceBuilder service;

	public XSDSparkSchemaServiceBuilderTest() throws Exception
	{
		String fullPath = jaxbSources.toAbsolutePath().toString();
		logger.info("Reading the full path : " + fullPath);
		service = new XSDSparkSchemaServiceBuilder(jaxbSources);
	}

	@Test
	public void testBuildFullService() throws Exception
	{
		final XSDSparkSchemaService schemaService = service.buildServiceForFullTypes();
		Assertions.assertNotNull(schemaService);
	}

}
