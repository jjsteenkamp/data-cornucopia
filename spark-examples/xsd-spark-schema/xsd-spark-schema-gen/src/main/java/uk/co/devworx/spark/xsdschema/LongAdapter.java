package uk.co.devworx.spark.xsdschema;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * 
 *
 */

public class LongAdapter extends XmlAdapter<String, Long> {

	DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	
	@Override
	public Long unmarshal(String v) {
		if (Objects.nonNull(v)) {
			try {
				return Long.valueOf(v);
			} catch (Exception e) {
				throw new RuntimeException("Failed to parse boolean: " + v, e);
			}
		}
		return null;
	}

	@Override
	public String marshal(Long v) {
		if (Objects.nonNull(v)) {
			return String.valueOf(v);
		}
		return null;
	}
}
