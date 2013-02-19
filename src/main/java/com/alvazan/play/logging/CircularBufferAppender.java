package com.alvazan.play.logging;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;

public class CircularBufferAppender extends AppenderBase<ILoggingEvent> implements AppenderAttachable<ILoggingEvent> {

	private AppenderAttachableImpl<ILoggingEvent> aai = new AppenderAttachableImpl<ILoggingEvent>();
	private int appenderCount;
	private int bufferSize = 2000;
	private Level flushThreshold = Level.WARN;

	private List<ILoggingEvent> events = new ArrayList<ILoggingEvent>();

	public void addAppender(Appender<ILoggingEvent> newAppender) {
		if (appenderCount == 0) {
			appenderCount++;
			addInfo("Attaching appender named [" + newAppender.getName()
					+ "] to appender ["+getName()+"].");
			aai.addAppender(newAppender);
		} else {
			addWarn("One and only one appender may be attached to AsyncAppender name=["+getName()+"].");
			addWarn("Ignoring additional appender named ["
					+ newAppender.getName() + "]");
		}
	}

	public void setFlushThreshold(String level) {
		this.flushThreshold = Level.toLevel(level);
	}

	public void setBufferSize(int size) {
		this.bufferSize = size;
	}

	@Override
	public void start() {
		if (appenderCount == 0) {
			addError("No attached appenders found.");
			return;
		}
		
		if (bufferSize < 1) {
			addError("Invalid buffer size [" + bufferSize + "]");
			return;
		}
		super.start();
	}

	
	@Override
	public void stop() {
		super.stop();
		aai.detachAndStopAllAppenders();
	}

	@Override
	protected void append(ILoggingEvent evt) {
		events.add(evt);

		// if past the threshold, then shove the last 200 logs inside there...
		if (evt.getLevel().isGreaterOrEqual(flushThreshold)) {
			for (ILoggingEvent theEvent : events) {
				aai.appendLoopOnAppenders(theEvent);
			}
			
			events.clear();
		}
	}

	public Iterator<Appender<ILoggingEvent>> iteratorForAppenders() {
		return aai.iteratorForAppenders();
	}

	public Appender<ILoggingEvent> getAppender(String name) {
		return aai.getAppender(name);
	}

	public boolean isAttached(Appender<ILoggingEvent> appender) {
		return aai.isAttached(appender);
	}

	public void detachAndStopAllAppenders() {
		aai.detachAndStopAllAppenders();
	}

	public boolean detachAppender(Appender<ILoggingEvent> appender) {
		return aai.detachAppender(appender);
	}

	public boolean detachAppender(String name) {
		return aai.detachAppender(name);
	}	
}
