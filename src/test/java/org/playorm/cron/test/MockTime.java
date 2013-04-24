package org.playorm.cron.test;

import org.joda.time.DateTime;
import org.playorm.cron.impl.CurrentTime;

public class MockTime implements CurrentTime {

	private DateTime time;

	public void addReturnTime(int millis) {
		time = new DateTime(millis);
	}

	@Override
	public DateTime currentTime() {
		if(time == null) {
			throw new IllegalStateException("call addREturnTime from test case first");
		}
		DateTime temp = time;
		time = null;
		return temp;
	}

}
