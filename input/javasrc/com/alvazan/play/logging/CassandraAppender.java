package com.alvazan.play.logging;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.joda.time.LocalDateTime;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.base.spi.UniqueKeyGenerator;

public class CassandraAppender extends AppenderBase<ILoggingEvent> implements
		AppenderAttachable<ILoggingEvent> {

	private AppenderAttachableImpl<ILoggingEvent> aai = new AppenderAttachableImpl<ILoggingEvent>();
	private int appenderCount;
	private static String hostname = UniqueKeyGenerator.getHostname();
	private int maxLogsEachServer = 100000;
	private int batchSize = 50;
	private int counter;
	private List<LogEvent> inMemoryBuffer = new ArrayList<LogEvent>();

	private int errorCount = 0;
	private static boolean inTryCatch;

	private static NoSqlEntityManagerFactory factory;

	public static void setFactory(NoSqlEntityManagerFactory f) {
		factory = f;
	}
	
	public static boolean isInTryCatch() {
		return inTryCatch;
	}

	public void addAppender(Appender<ILoggingEvent> newAppender) {
		if (appenderCount == 0) {
			appenderCount++;
			addInfo("Attaching appender named [" + newAppender.getName()
					+ "] to appender [" + getName() + "].");
			aai.addAppender(newAppender);
		} else {
			addWarn("One and only one appender may be attached to CassandraAppender name=["
					+ getName() + "].");
			addWarn("Ignoring additional appender named ["
					+ newAppender.getName() + "]");
		}
	}

	@Override
	public void start() {
		if (appenderCount == 0) {
			addError("No attached appenders found.");
			return;
		}

		super.start();
	}

	@Override
	protected void append(ILoggingEvent evt) {
		if (factory == null) {
			aai.appendLoopOnAppenders(evt);
			errorCount++;
			if (errorCount > 200) {
				addError("You really need to call CassandraAppender.setFactory to use the Cassandra appender");
			}
			return;
		} else if(inTryCatch)
			return; //don't log while in try catch

		LogEvent logEvt = new LogEvent();
		logEvt.setLevel("" + evt.getLevel());
		logEvt.setLogger(evt.getLoggerName());
		logEvt.setMessage(evt.getMessage());
		logEvt.setStackTrace(fullDump(evt));

		LocalDateTime t = new LocalDateTime(evt.getTimeStamp());
		logEvt.setTime(t);

		Map<String, String> map = evt.getMDCPropertyMap();
		String sid = map.get("sessionid");
		logEvt.setSessionId(sid, 2);
		String user = map.get("user");
		logEvt.setUser(user);
		logEvt.setThreadName(evt.getThreadName());

		insert(logEvt);
	}

	private synchronized void insert(LogEvent logEvt) {
		
		if (counter > maxLogsEachServer)
			counter = 0;

		String id = hostname + counter;
		logEvt.setId(hostname, counter);
		inMemoryBuffer.add(logEvt);

		counter++;

		if (inMemoryBuffer.size() >= batchSize) {
			flushEvents();
			inMemoryBuffer.clear();
		}
	}

	public String fullDump(ILoggingEvent evt) {
		try {
			IThrowableProxy proxy = evt.getThrowableProxy();
			if(proxy == null)
				return null;
			
			StringBuilder builder = new StringBuilder();
			for (StackTraceElementProxy step : proxy
					.getStackTraceElementProxyArray()) {
				String string = step.toString();
				builder.append(CoreConstants.TAB).append(string);
				ThrowableProxyUtil.subjoinPackagingData(builder, step);
				builder.append(CoreConstants.LINE_SEPARATOR);
			}
			return builder.toString();
		} catch(Exception e) {
			addError("exception trying to log exception", e);
			return "exception parsing exception";
		}
	}
	
	public void flushEvents() {
		try {
			inTryCatch = true;
			flushEventsImpl();
		} finally {
			inTryCatch = false;
		}
	}
	public void flushEventsImpl() {
		NoSqlEntityManager mgr = factory.createEntityManager();

		for (LogEvent evt : inMemoryBuffer) {
			mgr.put(evt, false);
		}

		ServersThatLog log = new ServersThatLog();
		log.setId(ServersThatLog.THE_ONE_KEY);
		log.getServers().add(hostname);
		mgr.put(log);
		
		mgr.flush();
		mgr.clear();
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
