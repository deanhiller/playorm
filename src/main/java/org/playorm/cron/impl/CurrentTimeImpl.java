package org.playorm.cron.impl;

import org.joda.time.DateTime;

public class CurrentTimeImpl implements CurrentTime {

	@Override
	public DateTime currentTime() {
		return new DateTime();
	}

}
