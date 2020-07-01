package uk.co.devworx.jdbc.incrementer;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class SQLRunners
{
	@XmlElement
	public List<SQLRunner> runners = new ArrayList<>();
}

class SQLRunner
{
	
	@XmlElement
	public String jdbcDriver;
	@XmlElement
	public String jdbcUrl;
	@XmlElement
	public String jdbcUsername;
	@XmlElement
	public String jdbcPassword;
	
	@XmlElement
	public List<ReplaceParameter> replaceParameter = new ArrayList<>();
	
	@XmlElement
	public List<SQLScript> sqls = new ArrayList<>();
	
}

class ReplaceParameter 
{
	@XmlAttribute
	public String key;
	
	public String value;
}

class SQLScript 
{
	@XmlAttribute
	public String csvFile;
	
	@XmlAttribute
	public Boolean append;
	
	public String value;
}
















