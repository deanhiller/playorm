package com.alvazan.play.logging;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.joda.time.LocalDateTime;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.spi.AppenderAttachableImpl;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.base.spi.UniqueKeyGenerator;

public class CassandraAppender extends UnsynchronizedAppenderBase<ILoggingEvent>{

	private AppenderAttachableImpl<ILoggingEvent> aai = new AppenderAttachableImpl<ILoggingEvent>();
	private int appenderCount;
	private static String hostname = UniqueKeyGenerator.getHostname();
	private int maxLogsEachServer = 100000;
	private int batchSize = 10;
	private int counter;
	private List<LogEvent> inMemoryBuffer = new ArrayList<LogEvent>();

	private int errorCount = 0;

	private static NoSqlEntityManagerFactory factory;
	
	public static void setFactory(NoSqlEntityManagerFactory f) {
		factory = f;
	}

	public void addAppender(Appender<ILoggingEvent> newAppender) {
		if (appenderCount == 0) {
			appenderCount++;
			addInfo("Attaching appender named [" + newAppender.getName()
					+ "] to appender ["+getName()+"].");
			aai.addAppender(newAppender);
		} else {
			addWarn("One and only one appender may be attached to CassandraAppender name=["+getName()+"].");
			addWarn("Ignoring additional appender named ["
					+ newAppender.getName() + "]");
		}
	}

	@Override
	protected void append(ILoggingEvent evt) {
		if(factory == null) {
			aai.appendLoopOnAppenders(evt);
			errorCount++;
			if(errorCount > 200) {
				addError("You really need to call CassandraAppender.setFactory to use the Cassandra appender");
			}
			return;
		}
		
		LogEvent logEvt = new LogEvent();
		logEvt.setLevel(""+evt.getLevel());
		logEvt.setLogger(evt.getLoggerName());
		logEvt.setMessage(evt.getMessage());
		logEvt.setStackTrace(fullDump(evt));
		
		LocalDateTime t = new LocalDateTime(evt.getTimeStamp());
		logEvt.setTime(t);
		
		logEvt.setServerName(hostname);
		
		Map<String, String> map = evt.getMDCPropertyMap();
		String sid = map.get("sid");
		logEvt.setSessionId(sid, 2);
		String user = map.get("user");
		logEvt.setUser(user);
		logEvt.setThreadName(evt.getThreadName());
		
		insert(logEvt);
	}

	private synchronized void insert(LogEvent logEvt) {
		if(counter > maxLogsEachServer)
			counter = 0;

		String id = hostname+counter;
		logEvt.setId(id);
		inMemoryBuffer.add(logEvt);
		
		counter++;
		
		if(inMemoryBuffer.size() >= batchSize) {
			flushEvents();
			inMemoryBuffer.clear();
		}
	}

	public String fullDump(ILoggingEvent evt) {
		IThrowableProxy proxy = evt.getThrowableProxy();
		
	    StringBuilder builder = new StringBuilder();
	    for (StackTraceElementProxy step : proxy.getStackTraceElementProxyArray()) {
	      String string = step.toString();
	      builder.append(CoreConstants.TAB).append(string);
	      ThrowableProxyUtil.subjoinPackagingData(builder, step);
	      builder.append(CoreConstants.LINE_SEPARATOR);
	    }
	    return builder.toString();
	}	
	

	public void flushEvents() {
		NoSqlEntityManager mgr = factory.createEntityManager();
				
		for(LogEvent evt : inMemoryBuffer) {
			mgr.put(evt);
		}

		mgr.flush();
		mgr.clear();
	}
}
