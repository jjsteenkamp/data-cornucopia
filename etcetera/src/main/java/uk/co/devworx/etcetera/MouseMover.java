package uk.co.devworx.etcetera;

import java.awt.*;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simple class to move the mouse around - to prevent screen locking. 
 * 
 * @author jsteenkamp
 *
 */
public class MouseMover
{
	public static final int DELAY_MILLISECONDS = 1000 * 60 * 3; //Every 3 minutes.

	private static final AtomicBoolean RUN_MOUSE_MOVER = new AtomicBoolean(true);

	private static final SecureRandom rnd = new SecureRandom();

	public static void main(String... args) throws Exception 
	{
		final Robot robot = new Robot();
		
		while(RUN_MOUSE_MOVER.get())
		{
			final PointerInfo pointerInfo = MouseInfo.getPointerInfo();
			final Point location = pointerInfo == null ? (null) : pointerInfo.getLocation();

			final int x = (location == null) ? 0 : location.x;
			final int y = (location == null) ? 0 : location.y;
			final boolean upOrDown = rnd.nextBoolean();

			int newX = x + (upOrDown ? 1 : -1);
			
			if(upOrDown == true)
			{
				robot.mouseMove(newX, y);
			}
			
			System.out.println("Moved the Mouse to Location : " + x + ", " + y + " - the positional move was up = " + upOrDown + " || Will now sleep for " + DELAY_MILLISECONDS + " milliseconds.");
			
			TimeUnit.MILLISECONDS.sleep(DELAY_MILLISECONDS);
		}
		
	}

	public static void abort()
	{
		RUN_MOUSE_MOVER.set(true);
	}


}
