package uk.co.devworx.spark.xsdschema;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;

/**
 * 
 *
 */

public class BooleanAdapter extends XmlAdapter<String, Boolean> {

	DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	
	@Override
	public Boolean unmarshal(String v) {
		if (Objects.nonNull(v)) {
			try {
				return Boolean.valueOf(v);
			} catch (Exception e) {
				throw new RuntimeException("Failed to parse boolean: " + v, e);
			}
		}
		return null;
	}

	@Override
	public String marshal(Boolean v) {
		if (Objects.nonNull(v)) {
			return String.valueOf(v);
		}
		return null;
	}
}
