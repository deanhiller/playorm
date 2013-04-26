package org.playorm.cron.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.playorm.cron.api.CronListener;
import org.playorm.cron.api.PlayOrmCronJob;
import org.playorm.cron.impl.db.MonitorDbo;
import org.playorm.cron.impl.db.WebNodeDbo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.Cursor;

public class CheckClusterRunnable implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(CheckClusterRunnable.class);
	
	@Inject
	private Config config;
	@Inject
	private HashGenerator hashGen;
	@Inject
	private CurrentTime time;
	
	private NoSqlEntityManagerFactory factory;

	private CronListener listener;

	@Override
	public void run() {
		try {
			if(log.isDebugEnabled())
				log.debug("firing cluster runnable");
			runImpl();
		} catch(Exception e) {
			if (log.isWarnEnabled())
				log.warn("Exception", e);
		}
	}
	public void runImpl() {
		if(listener == null)
			return; //no need to do anything
		
		NoSqlEntityManager mgr = factory.createEntityManager();
		Cursor<KeyValue<WebNodeDbo>> cursor = WebNodeDbo.findAllNodes(mgr);
		List<WebNodeDbo> all = new ArrayList<WebNodeDbo>();
		List<WebNodeDbo> servers = new ArrayList<WebNodeDbo>();
		while(cursor.next()) {
			KeyValue<WebNodeDbo> kv = cursor.getCurrent();
			WebNodeDbo val = kv.getValue();
			all.add(val);
			if(log.isDebugEnabled())
				log.debug("checking node="+val.getWebServerName());
			if(isServerUp(mgr, val)) {
				if(log.isDebugEnabled())
					log.debug("server is up="+val.getWebServerName());
				servers.add(val);
			}
			if(val.getWebServerName().equals(config.getHostName())) {
				if(log.isDebugEnabled())
					log.debug("saving our node to be up="+val.getWebServerName());
				saveNodeIsUp(mgr, val);
			}
		}

		mgr.clear();
		
		Collections.sort(servers, new ServerComparator());
		int serverNumber = -1;
		for(int i = 0; i < servers.size(); i++) {
			WebNodeDbo node = servers.get(i);
			if(node.getWebServerName().equals(config.getHostName())) {
				if(log.isDebugEnabled())
					log.debug("we are server number="+i+" out of number="+servers.size());
				serverNumber = i;
				break;
			}
		}

		if(serverNumber == -1)
			throw new IllegalStateException("serverNumber not found in list of servers="+all);
		
		runOurMonitors(mgr, servers.size(), serverNumber);
	}

	private void saveNodeIsUp(NoSqlEntityManager mgr, WebNodeDbo val) {
		val.setLastSeen(new DateTime());
		val.setUp(true);
		mgr.put(val);
		mgr.flush();
	}
	
	private void runOurMonitors(NoSqlEntityManager mgr, int numUpWebNodes, int serverNumber) {
		if (log.isInfoEnabled())
			log.info("num up nodes="+numUpWebNodes+" servernum="+serverNumber);
		Cursor<KeyValue<MonitorDbo>> cursor = MonitorDbo.findAll(mgr);
		while(cursor.next()) {
			KeyValue<MonitorDbo> kv = cursor.getCurrent();
			MonitorDbo val = kv.getValue();
			String id = val.getId();
			int hash = id.hashCode();
			int serverNum = hashGen.generate(hash, numUpWebNodes);
			if (log.isInfoEnabled())
				log.info("monitor="+val.getId()+" target server num="+serverNum+" our servernum="+serverNumber);
			if(serverNum == serverNumber) 
				processMonitor(mgr, val);
		}
	}

	private void processMonitor(NoSqlEntityManager mgr, MonitorDbo monitor) {
		DateTime now = time.currentTime();
		boolean shouldRun = calculateShouldRun(mgr, monitor, now);
		if(shouldRun) 
			runMonitor(mgr, monitor, now);
		

	}
	
	private boolean calculateShouldRun(NoSqlEntityManager mgr, MonitorDbo monitor, DateTime now) {
		DateTime lastRunTime = monitor.getLastRun();
		if(log.isDebugEnabled())
			log.debug("now="+now+" and lastrun time="+lastRunTime+" for monitor="+monitor.getId());

		if(lastRunTime == null) {
			return isInRunWindow(monitor, now);
		} else if(nextRuntimeHasPassed(lastRunTime, monitor, now))
			return true;

		return false;
	}

	private boolean nextRuntimeHasPassed(DateTime lastRunTime,
			MonitorDbo monitor, DateTime now) {
		//subtract 1000 or 1 second in case they line up on the minute intervals so we fire every two minutes if
		//they choose 2 minutes
		long timePeriod = monitor.getTimePeriodMillis();
		if(monitor.getEpochOffset() == null) {
			DateTime nextRun = lastRunTime.plus(timePeriod-1000);
			if(now.isAfter(nextRun))
				return true;
			return false;
		}

		DateTime lastShouldRun = monitor.getLastShouldHaveRun();
		DateTime nextRun = lastShouldRun.plus(timePeriod);
		if(now.isAfter(nextRun.minus(1000))) {
			monitor.setLastShouldHaveRun(nextRun);
			return true;
		}
		return false;
	}

	private boolean isInRunWindow(MonitorDbo monitor, DateTime now) {
		if(monitor.getEpochOffset() == null) {
			//If there is no epoch offset, we just start when the server starts 
			return true;
		}
		
		long half = config.getRate() / 2;
		long nowMillis = now.getMillis();
		long range = nowMillis - monitor.getEpochOffset();
		long multiplier = range / monitor.getTimePeriodMillis();
		long timePoint1 = monitor.getEpochOffset() + multiplier*monitor.getTimePeriodMillis();
		long timePoint2 = timePoint1+monitor.getTimePeriodMillis();
		long theTime = timePoint1;
		if(Math.abs(timePoint2-nowMillis) < Math.abs(timePoint1-nowMillis))
			theTime = timePoint2;
		
		if(Math.abs(nowMillis-theTime) < half) {
			DateTime t = new DateTime(theTime);
			monitor.setLastShouldHaveRun(t);
			return true;
		}

		return false;
	}

	private void runMonitor(NoSqlEntityManager mgr, MonitorDbo monitor,
			DateTime now) {
		if(log.isDebugEnabled())
			log.debug("run monitor="+monitor.getId());
		PlayOrmCronJob p = CopyUtil.copy(monitor);
		fireToListener(p);
		monitor.setLastRun(now);
		mgr.put(monitor);
		mgr.flush();
	}

	private void fireToListener(PlayOrmCronJob monitor) {
		try {
			listener.monitorFired(monitor);
		} catch(Exception e) {
			if (log.isWarnEnabled())
				log.warn("Listener threw an exception, check your client code for a bug(we catch, log and continue)", e);
		}
	}
	
	private boolean isServerUp(NoSqlEntityManager mgr, WebNodeDbo val) {
		long rateInMillis = config.getRate();
		DateTime lastSeen = val.getLastSeen();
		
		DateTime now = new DateTime();
		DateTime before = now.minus(rateInMillis);
		//give it 15 seconds before it really should be up to date in the table
		before = before.minusSeconds(15);
		
		if(lastSeen.isBefore(before)) {
			return false;
		}
		return true;
	}

	public void setFactory(NoSqlEntityManagerFactory factory) {
		this.factory = factory;
	}
	public void setListener(CronListener listener) {
		this.listener = listener;
	}

}
