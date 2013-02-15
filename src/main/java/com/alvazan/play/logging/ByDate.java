package com.alvazan.play.logging;

import java.util.Comparator;

import org.joda.time.LocalDateTime;

public class ByDate implements Comparator<LogEvent> {

	@Override
	public int compare(LogEvent o1, LogEvent o2) {
		LocalDateTime t1 = o1.getTime();
		LocalDateTime t2 = o2.getTime();
		
		if(t1.isBefore(t2))
			return 1;
		else if(t1.isAfter(t2))
			return -1;
		return 0;
	}

}
