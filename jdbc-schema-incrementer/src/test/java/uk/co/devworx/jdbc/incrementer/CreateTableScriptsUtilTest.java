package uk.co.devworx.jdbc.incrementer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Tests out various 'parsing' items.
 */
public class CreateTableScriptsUtilTest
{
	private static final Logger logger = LogManager.getLogger(CreateTableScriptsUtilTest.class);

	@Test
	public void testParsingOfScripts() throws Exception
	{
		Path p1 = Paths.get("src/test/resources/sql-schema/create-script-parsing/invalid.sql");
		Path p2 = Paths.get("src/test/resources/sql-schema/create-script-parsing/valid.sql");

		CreateTableScriptsUtil sc1 = CreateTableScriptsUtil.getInstance(p1);
		CreateTableScriptsUtil sc2 = CreateTableScriptsUtil.getInstance(p2);

		Assertions.assertTrue(sc2.getErrors().isEmpty());
		Assertions.assertFalse(sc1.getErrors().isEmpty());

		logger.info("ERRORS - SC2 : " + sc2.getErrors());

		String schemaName = sc2.getSchemaName();
		String tableName = sc2.getTableName();

		Assertions.assertEquals("${TARGET_DB}",schemaName);
		Assertions.assertEquals("${NAMESPACE}tbl_rainbow_schema", tableName);

		logger.info("TRANSIENT TABLE SCRIPT : ");

		CreateTableScriptsUtil.CreateTransientTableScript trans = sc2.createTransientScript();

		logger.info(trans.getUnresolvedCreateTableScript());

	}

	@Test
	public void testGetTransientSuffix()
	{
		final String suffix = CreateTableScriptsUtil.getTransientTableSuffix();
		logger.info("Suffix : " + suffix);
	}



}
