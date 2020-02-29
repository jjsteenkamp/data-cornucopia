package uk.co.devworx.jdbc;

import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SQLRunnersCreateTest
{
	private static final Logger logger = LogManager.getLogger(SQLRunnersCreateTest.class);
	
	@Test
	public void testCreateExampleXMLFileAndRead() throws Exception
	{
		SQLRunners root = new SQLRunners();
		
		SQLRunner sqlRunner = new SQLRunner();
		sqlRunner.jdbcDriver = "jdbc-driver";
		sqlRunner.jdbcUrl = "jdbcUrl";
		sqlRunner.jdbcUsername = "jdbcUsername";
		sqlRunner.jdbcPassword = "jdbcPassword";
		
		SQLScript sql1 = new SQLScript();
		sql1.csvFile = "some.csv.file.csv";
		sql1.value = "Select * from Test_table";
		
		SQLScript sql2 = new SQLScript();
		sql2.csvFile = "some.other.csv.file.csv";
		sql2.value = "Select * from Another_test";
		
		sqlRunner.sqls.add(sql1);
		sqlRunner.sqls.add(sql2);
		
		ReplaceParameter rplParam1 = new ReplaceParameter();
		rplParam1.key = "##VAL_1##";
		rplParam1.value = "X";
		
		sqlRunner.replaceParameter.add(rplParam1);
		
		root.runners.add(sqlRunner);
		
		JAXBContext jaxbContext = JAXBContext.newInstance(SQLRunners.class);
		Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
		Unmarshaller jaxbUnMarshaller = jaxbContext.createUnmarshaller();

		// output pretty printed
		jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

		StringWriter writer = new StringWriter();
		jaxbMarshaller.marshal(root, writer);
		writer.close();
		
		logger.info(writer.toString());
		
		Object unmarshal = jaxbUnMarshaller.unmarshal(new StringReader(writer.toString()));
		
		Assertions.assertNotNull(unmarshal);
		
	}

}
