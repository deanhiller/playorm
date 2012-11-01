package com.alvazan.play;

import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import play.Play;
import play.PlayPlugin;
import play.mvc.Http.Request;
import play.mvc.Http.Response;
import play.mvc.Scope.Session;
import play.vfs.VirtualFile;

public class AddLogContextPlugin extends PlayPlugin {

	private static final Logger log = LoggerFactory.getLogger(AddLogContextPlugin.class);
	private static long lastTimeStamp;
	private boolean isProduction;
	private ThreadLocal<Long> startTime = new ThreadLocal<Long>();
	
	public AddLogContextPlugin() {
		String mode = Play.configuration.getProperty("application.mode");
		if("prod".equals(mode)) {
			isProduction = true;
		}
	}
	static {
		LocalDateTime time = new LocalDateTime(2012, 6, 1, 0, 0);
		long baseTime = time.toDate().getTime();
		lastTimeStamp = System.currentTimeMillis() - baseTime;
	}
	
	private synchronized long fetchLast() {
		return ++lastTimeStamp;
	}
	
	@Override
	public void routeRequest(Request request) {
		beginRequest();
		super.routeRequest(request);
	}

	private void beginRequest() {
		long start = System.currentTimeMillis();
		startTime.set(start);
		
		Session session = Session.current();
		if(session != null) {
			setupMDC(session);
		}
		
		Request current = Request.current();
		if(current != null) {
			if(isProduction || (!current.path.startsWith("/public")))
				log.info("---begin request="+current.method+":"+current.path);
		}
	}

	private void setupMDC(Session session) {
		if(session.get("sid") == null) {
			session.put("sid", fetchLast()+"");
		}

		String sid = session.get("sid");
		String username = session.get("username");
		MDC.put("sessionid", sid);
		MDC.put("user", ""+username);
	}

	@Override
	public boolean serveStatic(VirtualFile file, Request request,
			Response response) {
		beginRequest();
		return super.serveStatic(file, request, response);
	}

	
	@Override
	public void invocationFinally() {
		Long start = startTime.get();
		//start will be null IF StartupBean was just invoked!!!! 
		if(start != null) {
			Request current = Request.current();
			long total = System.currentTimeMillis() - start;
			startTime.set(null);
			if(isProduction || (!current.path.startsWith("/public")))
				log.info("---ended request="+current.method+":"+current.path+" total time="+total+" ms");
		}
		
		MDC.put("sessionid", "");
		MDC.put("user", "");
	}
	
}
