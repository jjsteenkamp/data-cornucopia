package uk.co.devworx.xmlstomper;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

public class XmlToCSVStomper
{
	private static final Logger logger = LogManager.getLogger(XmlToCSVStomper.class);
	
	public static final String BUSINESS_DATE_ATTRIBUTE_NAME = "businessDate";
	public static final String TIMESTAMP_ATTRIBUTE_NAME = "timeStamp";
	public static final String RECORD_VERSION_ATTRIBUTE_NAME = "recordVersion";
	public static final String TYPE_ATTRIBUTE_NAME = "type";
	
	public static final String BUSINESS_DATE_COLUMN = "BusinessDate";
	public static final String TIMESTAMP_COLUMN = "Timestamp";
	public static final String RECORD_VERSION_COLUMN = "RecordVersion";
	public static final String TYPE_COLUMN = "Type";
	public static final String FILENAME_COLUMN = "FileName";

	private static final SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();

	private static SAXParser getSAXParser()
	{
		try
		{
			SAXParser parser = saxParserFactory.newSAXParser();
			parser.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
			parser.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
			return parser;
		}
		catch (ParserConfigurationException | SAXException e)
		{
			throw new IllegalStateException("Encountered SAX Setup exception. : " + e, e);
		}
	}


	public static void main(String... args) throws Exception
	{
		if(args.length < 3)
		{
			throw new RuntimeException("Usage : <element-name-to-use> <input-directory> <csv-target-name>");
		}
		StompXmlFilesToCSV(args[0], args[1], args[2]);
	}
	
	public static String StompXmlFilesToCSV(String elementNameToUse, String inputDirectoryStr, String csvTargetNameStr) throws Exception
	{
		File inputDirectory = new File(inputDirectoryStr);
		File csvTargetName = new File(csvTargetNameStr);
		
		logger.info("Element to Use : " + elementNameToUse);
		logger.info("Input Directory : " + inputDirectory);
		logger.info("CSV Target : " + csvTargetName);
		
		if(inputDirectory.isDirectory() == false)
		{
			throw new RuntimeException(inputDirectory + " is not a directory");
		}
		
		final File[] allXmlOrGzFiles = inputDirectory.listFiles(new FileFilter()
		{
			@Override
			public boolean accept(File pathname)
			{
				return pathname.getName().endsWith(".xml") || pathname.getName().endsWith("gz");
			}
		});

		javax.xml.parsers.SAXParser saxParser = getSAXParser();
	     
		LinkedHashSet<String> allColumnNames = new LinkedHashSet<>();
		allColumnNames.add(FILENAME_COLUMN);
		allColumnNames.add(TIMESTAMP_COLUMN);
		allColumnNames.add(BUSINESS_DATE_COLUMN);
		allColumnNames.add(RECORD_VERSION_COLUMN);
		allColumnNames.add(TYPE_COLUMN);

		try(ColumnDeterminatorHandler handler = new ColumnDeterminatorHandler(elementNameToUse))
		{
			for (int i = 0; i < allXmlOrGzFiles.length; i++)
			{
				File xmlOrGz = allXmlOrGzFiles[i];
				logger.info("Column Determinator Processing (1st pass) : " + xmlOrGz + " - file " + (i + 1) + " out of " + allXmlOrGzFiles.length);
				try(InputStream ins = xmlOrGz.getName().endsWith(".gz") ? new GZIPInputStream(new FileInputStream(xmlOrGz)) : new FileInputStream(xmlOrGz))
				{
					BufferedInputStream xmlReader = new BufferedInputStream(ins);
					saxParser.parse(xmlReader, handler);
					xmlReader.close();
				}
			}
			allColumnNames.addAll(handler.getColumnNames());
		}
		
		logger.info("ALL COLUMNS:");
		allColumnNames.forEach(a -> 
		{
			logger.info(a);
		});
		logger.info("No Processing real files");
		
		saxParser = getSAXParser();
	        
		try(MyHandler handler = new MyHandler(csvTargetName, elementNameToUse, allColumnNames))
		{
			for (int i = 0; i < allXmlOrGzFiles.length; i++)
			{
				File xmlOrGz = allXmlOrGzFiles[i];

				logger.info("Processing : " + xmlOrGz + " - file " + (i + 1) + " out of " + allXmlOrGzFiles.length);

				try(InputStream ins = xmlOrGz.getName().endsWith(".gz") ? new GZIPInputStream(new FileInputStream(xmlOrGz)) : new FileInputStream(xmlOrGz);
					BufferedInputStream xmlReader = new BufferedInputStream(ins))
				{
					handler.setInputXMLFileName(xmlOrGz.getName());
					saxParser.parse(xmlReader, handler);
				}
			}
		}
		
		String msg =  ("ALL DONE - please check : " + csvTargetName.getAbsolutePath());
		logger.info(msg);
		return msg;
	}
	
	static class ColumnDeterminatorHandler extends DefaultHandler implements AutoCloseable
	{
		private final String elementNameToUse;		
		private volatile boolean insideTheElementToWatch;
		
		private volatile String currentElementName;
		private final AtomicReference<Set<String>> columnNames = new AtomicReference<>();
		
		public ColumnDeterminatorHandler(String elementNameToUse) throws IOException
		{
			columnNames.set(new LinkedHashSet<>());
			this.elementNameToUse = elementNameToUse;
		}
		
		public LinkedHashSet<String> getColumnNames()
		{
			return (LinkedHashSet<String>)columnNames.get();
		}

		@Override
		public void startElement(	String uri,
									String localName,
									String qName,
									Attributes attributes)
				throws SAXException
		{
			
			boolean hasJustBeenSet = false;
			
			if(qName.equals(elementNameToUse))
			{
				insideTheElementToWatch = true;
				hasJustBeenSet = true;
			}
			
			if(insideTheElementToWatch == true)
			{
				currentElementName = qName;
				if(hasJustBeenSet == false)
				{
					columnNames.get().add(currentElementName);
				}	
			}
		}

		@Override
		public void endElement(	String uri,
								String localName,
								String qName)
				throws SAXException
		{
			if(qName.equals(elementNameToUse))
			{
				insideTheElementToWatch = false;
			}
			
		}

		@Override
		public void characters(	char[] ch,
								int start,
								int length)
				throws SAXException
		{	
		}

		@Override
		public void close() throws Exception
		{
		}
		 
	}
	
	static class MyHandler extends DefaultHandler implements AutoCloseable
	{
		private final File targetCSVFile;
		
		private volatile String typeField = "";
		private volatile String recordVersionField = "";
		private volatile String timestampField = "";
		private volatile String businessDateField = "";
		
		private volatile String inputXMLFileName;
		
		private final CSVPrinter csvWriter;
		private final String elementNameToUse;
		
		private final LinkedHashSet<String> allColumnNames;
		
		private volatile boolean hasWrittenHeader;
		
		private volatile boolean insideTheElementToWatch;
		
		private volatile String currentElementName;
		private final AtomicReference<StringBuffer> currentElementValue = new AtomicReference<>();
		private final AtomicReference<LinkedHashMap<String, String>> columnValues = new AtomicReference<>();
		
		@Override
		public void endDocument() throws SAXException
		{
			super.endDocument();
			typeField = "";
			recordVersionField = "";
			timestampField = "";
			businessDateField = "";
		}

		public MyHandler(File targetCSVFileP, 
		                 String elementNameToUse, 
		                 LinkedHashSet<String> allColumnNames
		                 ) throws IOException
		{
			this.allColumnNames = allColumnNames;
			this.targetCSVFile = targetCSVFileP;
			this.elementNameToUse = elementNameToUse;
			this.hasWrittenHeader = hasWrittenHeader;
			this.csvWriter = new CSVPrinter(Files.newBufferedWriter(targetCSVFile.toPath()), CSVFormat.DEFAULT);
		}

		public void setInputXMLFileName(String inputXMLFileName)
		{
			this.inputXMLFileName = inputXMLFileName;
		}

		public String getTimestampField()
		{
			return timestampField;
		}

		public String getBusinessDateField()
		{
			return businessDateField;
		}

		@Override
		public void startElement(	String uri,
									String localName,
									String qName,
									Attributes attributes)
				throws SAXException
		{
			int length = attributes.getLength();
			for (int i = 0; i < length; i++)
			{
				String attQName = attributes.getQName(i);
				String attLocalName = attributes.getLocalName(i);

				if(attQName.equals(BUSINESS_DATE_ATTRIBUTE_NAME) || attLocalName.equals(BUSINESS_DATE_ATTRIBUTE_NAME))
				{
					businessDateField = attributes.getValue(i);
				}
				if(attQName.equals(TIMESTAMP_ATTRIBUTE_NAME) || attLocalName.equals(TIMESTAMP_ATTRIBUTE_NAME))
				{
					timestampField = attributes.getValue(i);
				}
				if(attQName.equals(RECORD_VERSION_ATTRIBUTE_NAME) || attLocalName.equals(RECORD_VERSION_ATTRIBUTE_NAME))
				{
					recordVersionField = attributes.getValue(i);
				}
				if(attQName.equals(TYPE_ATTRIBUTE_NAME) || attLocalName.equals(TYPE_ATTRIBUTE_NAME))
				{
					typeField = attributes.getValue(i);
				}	
			}
			
			if(qName.equals(elementNameToUse))
			{
				//logger.info("QName matches : " + elementNameToUse);
				insideTheElementToWatch = true;
				columnValues.set(new LinkedHashMap<>());
				allColumnNames.forEach(k -> 
				{
					columnValues.get().put(k, "");
				});				
			}
			
			if(insideTheElementToWatch == true)
			{
				//logger.info("Start : Inside The Element To Watch  -> Create a new ElementName & Value");
				currentElementName = qName;
				currentElementValue.set(new StringBuffer());
			}
		}

		@Override
		public void endElement(	String uri,
								String localName,
								String qName)
				throws SAXException
		{
			if(qName.equals(elementNameToUse))
			{
				try
				{
					insideTheElementToWatch = false;
					//Print out for now.
					if(hasWrittenHeader == false)
					{
						csvWriter.printRecord(columnValues.get().keySet());
						hasWrittenHeader = true;
					}
					csvWriter.printRecord(columnValues.get().values());
				} 
				catch (IOException e)
				{
					throw new RuntimeException("Unable to write to the CSV file : " + e, e);
				}
			}
			
			if(insideTheElementToWatch == true)
			{
				columnValues.get().put(currentElementName, currentElementValue.toString());
				columnValues.get().put(FILENAME_COLUMN, inputXMLFileName);
				columnValues.get().put(TIMESTAMP_COLUMN, getTimestampField());
				columnValues.get().put(BUSINESS_DATE_COLUMN, getBusinessDateField());
				columnValues.get().put(RECORD_VERSION_COLUMN, recordVersionField);
				columnValues.get().put(TYPE_COLUMN, typeField);
			}
			
		}

		@Override
		public void characters(	char[] ch,
								int start,
								int length)
				throws SAXException
		{
			if(insideTheElementToWatch == true) 
			{
				currentElementValue.get().append(ch, start, length);
			}
			
		}

		@Override
		public void close() throws Exception
		{
			csvWriter.close();
		}
	}
 
	
}
