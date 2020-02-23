package uk.co.devworx.etcetera;

import java.awt.MouseInfo;
import java.awt.Point;
import java.awt.PointerInfo;
import java.awt.Robot;
import java.awt.event.InputEvent;
import java.util.concurrent.TimeUnit;

/**
 * Simple class to click the mouse - e.g. like a dashboard of sorts. 
 * 
 * @author jsteenkamp
 *
 */
public class MouseClicker
{
	public static final int DELAY_MILLISECONDS = 1000 * 60 * 3; //Every 3 minutes.
	
	public static void main(String... args) throws Exception 
	{
		final Robot robot = new Robot();
		
		while(true)
		{
			PointerInfo pointerInfo = MouseInfo.getPointerInfo();
			Point location = pointerInfo.getLocation();
			
			int x = location.x;
			int y = location.y;
			
			robot.mousePress(InputEvent.BUTTON1_DOWN_MASK);
			TimeUnit.MILLISECONDS.sleep(200);
			robot.mouseRelease(InputEvent.BUTTON1_DOWN_MASK);
			
			System.out.println("Clicked the Mouse @ Location : " + x + ", " + y + " || Will now sleep for " + DELAY_MILLISECONDS + " milliseconds.");
			
			TimeUnit.MILLISECONDS.sleep(DELAY_MILLISECONDS);
		}
		
	}

}
