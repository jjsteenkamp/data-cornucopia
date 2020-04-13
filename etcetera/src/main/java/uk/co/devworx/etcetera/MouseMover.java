package uk.co.devworx.etcetera;

import java.awt.MouseInfo;
import java.awt.Point;
import java.awt.PointerInfo;
import java.awt.Robot;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Simple class to move the mouse around - to prevent screen locking. 
 * 
 * @author jsteenkamp
 *
 */
public class MouseMover
{
	public static final int DELAY_MILLISECONDS = 1000 * 60 * 3; //Every 3 minutes.
	
	public static void main(String... args) throws Exception 
	{
		final Robot robot = new Robot();
		
		while(true)
		{
			final PointerInfo pointerInfo = MouseInfo.getPointerInfo();
			final Point location = pointerInfo == null ? (null) : pointerInfo.getLocation();

			final int x = (location == null) ? 0 : location.x;
			final int y = (location == null) ? 0 : location.y;
			final boolean upOrDown = ThreadLocalRandom.current().nextBoolean();

			int newX = x + (upOrDown ? 1 : -1);
			
			if(upOrDown == true)
			{
				robot.mouseMove(newX, y);
			}
			
			System.out.println("Moved the Mouse to Location : " + x + ", " + y + " - the positional move was up = " + upOrDown + " || Will now sleep for " + DELAY_MILLISECONDS + " milliseconds.");
			
			TimeUnit.MILLISECONDS.sleep(DELAY_MILLISECONDS);
		}
		
	}

}
