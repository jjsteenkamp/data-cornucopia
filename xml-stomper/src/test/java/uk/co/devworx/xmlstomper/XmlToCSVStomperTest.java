package uk.co.devworx.xmlstomper;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

/**
 * Some test cases for the stomper.  
 * 
 * @author jsteenkamp
 *
 */
public class XmlToCSVStomperTest
{
	private static final Logger logger = LogManager.getLogger(XmlToCSVStomperTest.class);
	
	@Test
	public void testSimpleExample() throws Exception
	{
		Path csvTargetOutput = Paths.get("target/XmlToCSVStomperTest-Simple.csv");
		XmlToCSVStomper.StompXmlFilesToCSV("EquityIndexValues", "src/test/resources", csvTargetOutput.toString());
		
		try(BufferedReader bufR = Files.newBufferedReader(csvTargetOutput);
			CSVParser parser = CSVParser.parse(bufR, CSVFormat.DEFAULT))
		{
			parser.forEach(c -> 
			{
				logger.info(c);	
			});
		}
		
	}
	
	

}
