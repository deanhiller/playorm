package org.playorm.monitor.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.playorm.monitor.api.MonitorListener;
import org.playorm.monitor.api.PlayOrmMonitor;
import org.playorm.monitor.impl.db.MonitorDbo;
import org.playorm.monitor.impl.db.WebNodeDbo;
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
	
	private NoSqlEntityManagerFactory factory;

	private MonitorListener listener;

	@Override
	public void run() {
		try {
			log.info("firing cluster runnable");
			runImpl();
		} catch(Exception e) {
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
			if(isServerUp(mgr, val))
				servers.add(val);
		}

		mgr.clear();
		
		Collections.sort(servers, new ServerComparator());
		int serverNumber = -1;
		for(int i = 0; i < servers.size(); i++) {
			WebNodeDbo node = servers.get(i);
			if(node.getWebServerName().equals(config.getHostName())) {
				serverNumber = i;
				break;
			}
		}

		if(serverNumber == -1)
			throw new IllegalStateException("serverNumber not found in list of servers="+all);
		
		runOurMonitors(mgr, servers.size(), serverNumber);
	}

	private void runOurMonitors(NoSqlEntityManager mgr, int numUpWebNodes, int serverNumber) {
		log.info("num up nodes="+numUpWebNodes+" servernum="+serverNumber);
		Cursor<KeyValue<MonitorDbo>> cursor = MonitorDbo.findAll(mgr);
		while(cursor.next()) {
			KeyValue<MonitorDbo> kv = cursor.getCurrent();
			MonitorDbo val = kv.getValue();
			String id = val.getId();
			int hash = id.hashCode();
			int serverNum = hashGen.generate(hash, numUpWebNodes);
			log.info("monitor="+val.getId()+" server num="+serverNum+" our num="+serverNumber);
			if(serverNum == serverNumber) 
				processMonitor(mgr, val);
		}
	}

	private void processMonitor(NoSqlEntityManager mgr, MonitorDbo monitor) {
		DateTime time = monitor.getLastRun();
		DateTime now = new DateTime();
		if(time == null) {
			runMonitor(mgr, monitor, now);
			return;
		}

		DateTime nextRunTime = time.plus(monitor.getTimePeriodMillis());
		if(now.isAfter(nextRunTime)) {
			runMonitor(mgr, monitor, now);
		}
	}
	private void runMonitor(NoSqlEntityManager mgr, MonitorDbo monitor,
			DateTime now) {
		log.info("run monitor="+monitor.getId());
		PlayOrmMonitor p = CopyUtil.copy(monitor);
		fireToListener(p);
		monitor.setLastRun(now);
		mgr.put(monitor);
		mgr.flush();
	}

	private void fireToListener(PlayOrmMonitor monitor) {
		try {
			listener.monitorFired(monitor);
		} catch(Exception e) {
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
	public void setListener(MonitorListener listener) {
		this.listener = listener;
	}

}
