package uk.co.devworx.spark_examples.pushdown;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class InstantTimeTest
{
	@Test
	public void testGetHourOfDay() throws Exception
	{
		Instant now = Instant.now();

	}


	@Test
	public void truncateValuesTest() throws Exception
	{
		Instant now = Instant.now();

		Instant instant = now.truncatedTo(ChronoUnit.DAYS);

		System.out.println(instant);

		Instant instant1 = instant.plusSeconds(60 * 60 * 24);

		System.out.println(instant);

		Instant instant2 = instant1.minusSeconds(1);

		System.out.println(instant2);

		Instant instant3 = instant2.plusNanos(999);

		System.out.println(instant3);

	}



}
