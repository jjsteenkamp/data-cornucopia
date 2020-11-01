package io.terahelix.spear.xsd.tests.jaxb.fluent;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;

import javax.xml.bind.annotation.adapters.XmlAdapter;

/**
 
 */

public class DateTimeAdapter extends XmlAdapter<String, LocalDateTime> {

	String ISO_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS";
	String ISO_PATTERN_TZ = ISO_PATTERN + "XXX";
	DateTimeFormatter isoPattern = DateTimeFormatter.ofPattern(ISO_PATTERN);
	DateTimeFormatter isoPatternTz = DateTimeFormatter.ofPattern(ISO_PATTERN_TZ);
	
	@Override
	public LocalDateTime unmarshal(String v) {
		if (Objects.nonNull(v)) {
			try {
				return LocalDateTime.parse(v, isoPatternTz);
			} catch (DateTimeParseException e) {
				throw new RuntimeException("Failed to parse time: " + v, e);
			}
		}
		return null;
	}

	@Override
	public String marshal(LocalDateTime v) {
		if (Objects.nonNull(v)) {			
			return v.format(isoPattern);
		}
		return null;
	}
}
