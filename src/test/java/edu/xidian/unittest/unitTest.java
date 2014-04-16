
package edu.xidian.unittest;

import java.util.Timer;
import java.util.TimerTask;

import com.alibaba.cobar.util.TimeUtil;

import junit.framework.*;

public class unitTest extends TestCase{
	
	public void testTime()
	{
		System.out.println(Runtime.getRuntime().availableProcessors());
		
		final Timer timer = new Timer("Timer1",true);
		
		final long INTERVAL = 2L;
		timer.schedule(updateTime(), 0L, INTERVAL);
		
	}
	
	private TimerTask updateTime(){
		return new TimerTask(){
			@Override
			public void run(){
				TimeUtil.update();
			}
		};
	}
}
