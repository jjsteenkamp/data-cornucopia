package uk.co.devworx.excel;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class ExcelToCSVConverter
{
	private final static Logger logger = Logger.getLogger(ExcelToCSVConverter.class.getName());

	public static void main(String... args) throws Exception
	{
		if(args.length < 3)
		{
			System.err.println("This class required 3 parameters - \n" +
									   "[0] - The path to the excel file \n" +
									   "[1] - The name of the sheet to extract \n " +
									   "[2] - The target CSV file");
			System.exit(1);
		}

		logger.info("Performing Conversion - \n" +
				"[0] - The path to the excel file : " + args[0] + "\n" +
				"[1] - The name of the sheet to extract: "+ args[1] + "\n" +
				"[2] - The target CSV file: " + args[2] + "\n"
		);

		final Path inputPath =  Paths.get(args[0]);
		final Path targetPath = Paths.get(args[2]);

		ExcelToCSVConverter converter = new ExcelToCSVConverter(inputPath,
																args[1],
																targetPath);
		converter.performConversion();

		logger.info("Completed the writing to : " + targetPath.toAbsolutePath());

	}

	private final Path excelFile;
	private final String sheetToExtract;
	private final Path targetFile;

	ExcelToCSVConverter(final Path excelFile,
						final String sheetToExtract,
						final Path targetFile)
	{
		this.excelFile = excelFile;
		this.sheetToExtract = sheetToExtract;
		this.targetFile = targetFile;
	}

	//this.csvWriter = new CSVPrinter(, CSVFormat.DEFAULT);

	public void performConversion() throws IOException
	{

		try (	final InputStream inp = Files.newInputStream(excelFile);
				final CSVPrinter csvPrinter = new CSVPrinter(Files.newBufferedWriter(targetFile),CSVFormat.EXCEL))
		{
			final XSSFWorkbook wb = new XSSFWorkbook(inp);
			final XSSFSheet sheet = wb.getSheet(sheetToExtract);
			if(sheet == null)
			{
				throw new RuntimeException("Unable to find the sheet with name - '" + sheetToExtract + "' in the file : " + excelFile.toAbsolutePath());
			}

			int totalRows = 0;
			int totalCells = 0;
			final List<String> rowValues = new ArrayList<>();
			for (Row row : sheet)
			{
				totalRows++;
				rowValues.clear();
				for (Cell cell : row)
				{
					rowValues.add(extractCell(cell));
					totalCells++;
				}
				csvPrinter.printRecord(rowValues);
			}

			logger.info("Printed a total of : " + totalRows + " rows");
			logger.info("Printed a total of : " + totalCells + " cells");
		}
	}

	private static String extractCell(Cell cell)
	{
		CellType cellType = cell.getCellType();
		switch(cellType)
		{
		case NUMERIC:
			return String.valueOf(cell.getNumericCellValue());
		case FORMULA:
		case STRING:
			return cell.getStringCellValue();
		case ERROR:
		case _NONE:
		case BLANK:
			return "";
		case BOOLEAN:
			return String.valueOf(cell.getBooleanCellValue());
		default:
			throw new RuntimeException("Undefined Cell Type : " + cellType);
		}


	}



}


