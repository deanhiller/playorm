package com.alvazan.play.logging;

import org.joda.time.LocalDateTime;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlPartitionByThisField;
import com.alvazan.orm.api.base.anno.NoSqlQuery;
import com.alvazan.orm.api.z8spi.KeyValue;

@NoSqlEntity
@NoSqlQuery(name="findBySession", query="PARTITIONS l(:partId) SELECT l FROM TABLE as l where l.sessionId = :sessionId")
public class LogEvent {

	@NoSqlId(usegenerator=false)
	private String id;

	private String level;
	
	private String serverName;
	
	private String threadName;
	
	@NoSqlPartitionByThisField
	private String idLastDigits;
	
	private LocalDateTime time;

	@NoSqlIndexed
	private String sessionId;
	
	private String user;
	
	private String logger;
	
	private String message;

	private String stackTrace;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getStackTrace() {
		return stackTrace;
	}

	public void setStackTrace(String stackTrace) {
		this.stackTrace = stackTrace;
	}

	public String getLevel() {
		return level;
	}

	public void setLevel(String level) {
		this.level = level;
	}

	public LocalDateTime getTime() {
		return time;
	}

	public void setTime(LocalDateTime time) {
		this.time = time;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId, int numDigits) {
		this.sessionId = sessionId;

		idLastDigits = parse(sessionId, numDigits);
	}

	private static String parse(String sessionId2, int numDigits) {
		return 	sessionId2.substring(sessionId2.length()-numDigits);
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getLogger() {
		return logger;
	}

	public void setLogger(String logger) {
		this.logger = logger;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getIdLastDigits() {
		return idLastDigits;
	}

	public String getServerName() {
		return serverName;
	}

	public void setServerName(String serverName) {
		this.serverName = serverName;
	}

	public String getThreadName() {
		return threadName;
	}

	public void setThreadName(String threadName) {
		this.threadName = threadName;
	}

	public static final Iterable<KeyValue<LogEvent>> findBySession(NoSqlEntityManager mgr, String sessionId, int numDigits) {
		String partId = parse(sessionId, numDigits);
		Query<LogEvent> query = mgr.createNamedQuery(LogEvent.class, "findBySession");
		query.setParameter("partId", partId);
		query.setParameter("sessionId", sessionId);
		return query.getResultsIter(false);
	}
}
