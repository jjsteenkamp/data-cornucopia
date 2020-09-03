package uk.co.devworx.spark_examples.pushdown;

import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class DiskBackedListTest
{

	@Test
	public void testBasicList() throws Exception
	{
		List<String> lst = new DiskBackedList<>("target/disk-list/DiskBackedListTest");

		for (int i = 0; i < 10; i++)
		{
			lst.add(UUID.randomUUID().toString());
		}

		System.out.println(lst.size());


		System.out.println(lst.get(5));

		Iterator<String> items = lst.iterator();
		while(items.hasNext())
		{
			System.out.println(items.next());
		}

	}


}
