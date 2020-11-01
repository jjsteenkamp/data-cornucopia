package io.terahelix.spear.xsd.tests.jaxb.fluent;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;

import javax.xml.bind.annotation.adapters.XmlAdapter;

/**
 * 
 *
 */

public class TimeAdapter extends XmlAdapter<String, LocalTime> {

	DateTimeFormatter pattern = DateTimeFormatter.ofPattern("HH:mm:ss");
	
	@Override
	public LocalTime unmarshal(String v) {
		if (Objects.nonNull(v)) {
			try {
				return LocalTime.parse(v, pattern);
			} catch (DateTimeParseException e) {
				throw new RuntimeException("Failed to parse time: " + v, e);
			}
		}
		return null;
	}

	@Override
	public String marshal(LocalTime v) {
		if (Objects.nonNull(v)) {
			return v.format(pattern);
		}
		return null;
	}
}
