package uk.co.devworx.spark_examples.pushdown;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A one off utility to create a number of orders in the database
 */
public class CreateLargeOrdersDatabase
{
	//public static final int ORDERS_TO_CREATE = 10_000_000;
	//public static final int ORDERS_TO_CREATE = 1_000_000;
	public static final int ORDERS_TO_CREATE = 100_000;
	//public static final int ORDERS_TO_CREATE = 1_000;
	public static final int EVENTS_PER_ORDER = 3;
	public static final int BATCH_SIZE = 5000;

	public static final String[] ORDER_TYPES = { "Customer Order", "Exchange Order", "Other Order" } ;
	public static final String[] INSTRUMENTS = { "FX.USD.GBP","FX.AUD.GBP","FX.NZD.AUD","FX.USD.EUR", "FX.GBP.EUR" } ;
	public static final String[] DIRECTIONS = { "Bid","Offer" } ;
	public static final String[] STATE = { "Done","Pending"} ;
	public static final String[] STATUS = { "Active","Cancelled" } ;


	public static final String[] ORDER_EVENT_TYPE = {"Order Pending","Initial Order State","Order Active"};
	public static final String[] ORDER_EVENT_STATUS = {"Active","Pending","Cancelled"};

	public static void main(String... args) throws Exception
	{
		SQLConnectionUtils utils = SQLConnectionUtils.getDefaultInstance();
		try(Connection con = utils.getConnection();
			Statement stmt = con.createStatement();
		)
		{
			con.setAutoCommit(false);

			stmt.execute("DROP TABLE IF EXISTS orders ");

			String sql1 = ("CREATE TABLE orders\n" +
					"(\n" +
					"	_entityId 				VARCHAR(255) PRIMARY KEY,\n" +
					"	entity_time				TIMESTAMP,\n" +
					"	order_type				VARCHAR(100),\n" +
					"	instrument				VARCHAR(100),\n" +
					"	direction				VARCHAR(100),\n" +
					"	state					VARCHAR(100),\n" +
					"	status					VARCHAR(100),\n" +
					"	last_confirmed_amount	DECIMAL(20, 2),\n" +
					"	filled_amount			DECIMAL(20, 2)\n" +
					"	\n" +
					")");

			stmt.execute(sql1);

			System.out.println(sql1);

			stmt.execute("DROP TABLE IF EXISTS order_event ");

			String sql2 = ("CREATE TABLE order_event\n" +
					"(\n" +
					"	_order_event_id			 			VARCHAR(255) PRIMARY KEY,\n" +
					"	_entityId			 				VARCHAR(255),\n" +
					"	customer_order_base_price			DECIMAL(20, 2),\n" +
					"	event_type							VARCHAR(100),\n" +
					"	status								VARCHAR(100),\n" +
					"	dateTime							TIMESTAMP\n" +
					");\n" +
					"\n" +
					"\n" +
					" ");

			System.out.println(sql2);

			stmt.execute(sql2);

			stmt.execute("DROP TABLE IF EXISTS orders_order_event_join ");

			String sql3 = "\n" +
					"CREATE TABLE orders_order_event_join \n" +
					"(\n" +
					"	o_entityId 							VARCHAR(255),\n" +
					"	o_entity_time						TIMESTAMP,\n" +
					"	o_order_type						VARCHAR(100),\n" +
					"	o_instrument						VARCHAR(100),\n" +
					"	o_direction							VARCHAR(100),\n" +
					"	o_state								VARCHAR(100),\n" +
					"	o_status							VARCHAR(100),\n" +
					"	o_last_confirmed_amount				DECIMAL(20, 2),\n" +
					"	o_filled_amount						DECIMAL(20, 2),\n" +
					"	_order_event_id			 			VARCHAR(255),\n" +
					"	_entityId			 				VARCHAR(255),\n" +
					"	customer_order_base_price			DECIMAL(20, 2),\n" +
					"	event_type							VARCHAR(100),\n" +
					"	status								VARCHAR(100),\n" +
					"	dateTime							TIMESTAMP\n" +
					")";

			stmt.execute(sql3);

			System.out.println(sql3);

			con.commit();

			PreparedStatement p1 = con.prepareStatement("INSERT INTO ORDERS VALUES (?,?,?,?,?,?,?,?,?)");
			PreparedStatement p2 = con.prepareStatement("INSERT INTO ORDER_EVENT VALUES (?,?,?,?,?,?)");

			int batchSize = 0;
			int finalCount = 0;

			for (int i = 1; i <= ORDERS_TO_CREATE; i++)
			{

				batchSize++;
				finalCount = i;

				String entityId = UUID.randomUUID().toString();

				p1.clearParameters();
				p1.setString(1, entityId);
				p1.setTimestamp(2, getRandomTime());
				p1.setString(3, getRandomItem(ORDER_TYPES));
				p1.setString(4, getRandomItem(INSTRUMENTS));
				p1.setString(5, getRandomItem(DIRECTIONS));
				p1.setString(6, getRandomItem(STATE));
				p1.setString(7, getRandomItem(STATUS));
				p1.setBigDecimal(8, BigDecimal.valueOf(1_000_000));
				p1.setBigDecimal(9, BigDecimal.valueOf(5_00_000));

				p1.addBatch();

				for (int j = 0; j < EVENTS_PER_ORDER; j++)
				{
					p2.clearParameters();
					p2.setString(1, UUID.randomUUID().toString());
					p2.setString(2, entityId);
					p2.setBigDecimal(3, BigDecimal.valueOf(1_000_000));
					p2.setString(4, getRandomItem(ORDER_EVENT_TYPE));
					p2.setString(5, getRandomItem(ORDER_EVENT_STATUS));
					p2.setTimestamp(6, getRandomTime());

					p2.addBatch();
				}


				if(batchSize % BATCH_SIZE == 0)
				{
					batchSize = 0;
					System.out.println("Executing Batch of " + BATCH_SIZE + " - Total Rows now - " + i);

					p1.executeBatch();
					p2.executeBatch();

					con.commit();

					p1.clearBatch();
					p2.clearBatch();

					System.out.println("Done Batch of " + BATCH_SIZE + " - Total Rows now - " + i);

					ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM ORDERS ");
					rs.next();
					System.out.println("ORDER COUNT : " + rs.getObject("cnt"));
					rs.close();

					rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM ORDER_EVENT ");
					rs.next();
					System.out.println("ORDER EVENT : " + rs.getObject("cnt"));
					rs.close();

				}

			}

			System.out.println("Doing the final batch !! ");
			p1.executeBatch();
			p2.executeBatch();
			con.commit();

			p1.close();
			p2.close();

			System.out.println("ALL DONE! - ROWS : " + finalCount);

			ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM ORDERS ");
			rs.next();
			System.out.println("ORDER COUNT : " + rs.getObject("cnt"));
			rs.close();

			rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM ORDER_EVENT ");
			rs.next();
			System.out.println("ORDER EVENT : " + rs.getObject("cnt"));
			rs.close();

			stmt.execute(
					"\n" +
							"INSERT INTO orders_order_event_join\n" +
							"SELECT \n" +
							"	o._entityId AS o_entityId 						,\n" +
							"	o.entity_time AS o_entity_time					,\n" +
							"	o.order_type AS o_order_type					,\n" +
							"	o.instrument AS o_instrument					,\n" +
							"	o.direction AS o_direction		 				,\n" +
							"	o.state AS o_state								,\n" +
							"	o.status AS o_status							,\n" +
							"	o.last_confirmed_amount AS o_last_confirmed_amount			,\n" +
							"	o.filled_amount AS o_filled_amount  ,\n" +
							"	oe._order_event_id			 		,\n" +
							"	oe._entityId			 			,\n" +
							"	oe.customer_order_base_price		,\n" +
							"	oe.event_type						,\n" +
							"	oe.status							,\n" +
							"	oe.dateTime							\n" +
							"\n" +
							"FROM ORDERS o \n" +
							"INNER JOIN ORDER_EVENT oe \n" +
							"ON o._ENTITYID = oe._ENTITYID\n" +
							"");

			con.commit();

			rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM orders_order_event_join ");
			rs.next();
			System.out.println("orders_order_event_join : " + rs.getObject("cnt"));
			rs.close();

			System.out.println("SHUTTING DOWN");

			stmt.execute("CHECKPOINT");
			stmt.execute("SHUTDOWN COMPACT");

			System.out.println("BYE");



		}

	}

	private static String getRandomItem(String[] orderTypes)
	{
		int len = orderTypes.length;
		return orderTypes[ThreadLocalRandom.current().nextInt(len)];

	}

	public static final LocalDateTime START_DATE = LocalDateTime.of(2019, 1, 1,1,1,1);
	public static long SECONDS_SEED = 31556952 + (31556952 / 2);

	private static Timestamp getRandomTime()
	{
		return new Timestamp(START_DATE.plusSeconds(ThreadLocalRandom.current().nextLong(SECONDS_SEED)).toInstant(ZoneOffset.MIN).toEpochMilli());
	}

}
