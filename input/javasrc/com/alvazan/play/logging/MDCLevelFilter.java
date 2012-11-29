package com.alvazan.play.logging;

import org.slf4j.MDC;
import org.slf4j.Marker;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.turbo.MatchingFilter;
import ch.qos.logback.core.spi.FilterReply;

public class MDCLevelFilter extends MatchingFilter {

	private String mdcFilter;
	private String value;
	private Level level;

	@Override
	public FilterReply decide(Marker marker, Logger logger, Level level,
			String format, Object[] params, Throwable t) {
		if (mdcFilter == null) {
			return FilterReply.NEUTRAL;
		}

		if(CassandraAppender.isInTryCatch())
			return onMatch;
		
		String value = MDC.get(mdcFilter);
		if (this.value.equals(value)) {
			if(level.levelInt <= this.level.levelInt)
				return onMatch;
		}
		return onMismatch;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public void setMDCKey(String mdcKey) {
		this.mdcFilter = mdcKey;
	}
	public void setThisLevelOrBelow(Level level) {
		this.level = level;
	}
}
