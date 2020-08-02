package uk.co.devworx.etcetera;

import java.awt.MouseInfo;
import java.awt.Point;
import java.awt.PointerInfo;
import java.awt.Robot;
import java.awt.event.InputEvent;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simple class to click the mouse - e.g. like a dashboard of sorts. 
 *
 * @author jsteenkamp
 *
 */
public class MouseClicker
{
	public static final int DELAY_MILLISECONDS = 1000 * 60 * 3; //Every 3 minutes.

	private static final AtomicBoolean RUN_MOUSE_CLICKER = new AtomicBoolean(true);

	public static void main(String... args) throws Exception 
	{
		final Robot robot = new Robot();
		
		while(RUN_MOUSE_CLICKER.get())
		{
			PointerInfo pointerInfo = MouseInfo.getPointerInfo();
			Point location = pointerInfo == null ? (null) : pointerInfo.getLocation();

			int x = (location == null) ? -1 : location.x;
			int y = (location == null) ? -1 : location.y;

			robot.mousePress(InputEvent.BUTTON1_DOWN_MASK);
			TimeUnit.MILLISECONDS.sleep(200);
			robot.mouseRelease(InputEvent.BUTTON1_DOWN_MASK);
			
			System.out.println("Clicked the Mouse @ Location : " + x + ", " + y + " || Will now sleep for " + DELAY_MILLISECONDS + " milliseconds.");
			
			TimeUnit.MILLISECONDS.sleep(DELAY_MILLISECONDS);
		}
		
	}

	public static void abort()
	{
		RUN_MOUSE_CLICKER.set(true);
	}

}
