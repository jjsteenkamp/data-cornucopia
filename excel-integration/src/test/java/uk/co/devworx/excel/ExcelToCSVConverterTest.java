package uk.co.devworx.excel;


import org.junit.jupiter.api.Test;

/**
 *  Simple test case using the Financial sample from Microsoft -
 *  https://docs.microsoft.com/en-us/power-bi/sample-financial-download
 *
 */
public class ExcelToCSVConverterTest
{
	@Test
	public void testCSVExtraction() throws Exception
	{
		ExcelToCSVConverter.main("src/test/resources/FinancialSample.xlsx",
								 "Sheet1",
								 "target/FinancialSample.csv");
	}

}
