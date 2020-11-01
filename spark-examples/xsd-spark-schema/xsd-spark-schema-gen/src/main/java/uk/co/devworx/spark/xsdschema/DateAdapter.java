package io.terahelix.spear.xsd.tests.jaxb.fluent;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;

import javax.xml.bind.annotation.adapters.XmlAdapter;

/**
 * 
 *
 */

public class DateAdapter extends XmlAdapter<String, LocalDate> {

	DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	
	@Override
	public LocalDate unmarshal(String v) {
		if (Objects.nonNull(v)) {
			try {
				return LocalDate.parse(v, pattern);
			} catch (DateTimeParseException e) {
				throw new RuntimeException("Failed to parse time: " + v, e);
			}
		}
		return null;
	}

	@Override
	public String marshal(LocalDate v) {
		if (Objects.nonNull(v)) {
			return v.format(pattern);
		}
		return null;
	}
}
