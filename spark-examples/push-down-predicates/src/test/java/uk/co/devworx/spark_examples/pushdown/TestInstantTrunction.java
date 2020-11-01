package uk.co.devworx.spark_examples.pushdown;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;

public class TestInstantTrunction
{
	@Test
	public void testTruncateInstant()
	{
		Instant now = Instant.now();

		System.out.println(now);

		LocalDateTime now1 = LocalDateTime.now();

		LocalDateTime with = now1.with(LocalTime.MIN);

		System.out.println(with);

		Instant instant = now.truncatedTo(ChronoUnit.DAYS);

		System.out.println("Instant : " + instant);

		//Instant instant2 = now.with(LocalTime.MIN);
		//System.out.println("Instant : " + instant2);


	}



}
