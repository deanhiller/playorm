package org.playorm.monitor.impl;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.playorm.monitor.api.MonitorListener;
import org.playorm.monitor.api.MonitorService;
import org.playorm.monitor.api.PlayOrmMonitor;
import org.playorm.monitor.impl.db.MonitorDbo;
import org.playorm.monitor.impl.db.WebNodeDbo;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;

public class MonitorServiceImpl implements MonitorService {

	@Inject
	private ScheduledExecutorService svc;
	@Inject
	private Config config;
	@Inject
	private CheckClusterRunnable clusterRunnable;
	
	private NoSqlEntityManagerFactory factory;
	
	@Override
	public void start() {
		String host = config.getHostName();
		WebNodeDbo node = new WebNodeDbo();
		node.setLastSeen(new DateTime());
		node.setWebServerName(host);
		node.setUp(true);
		
		NoSqlEntityManager mgr = factory.createEntityManager();
		mgr.put(node);
		mgr.flush();
		
		clusterRunnable.setFactory(factory);
		svc.scheduleAtFixedRate(clusterRunnable, 30000, config.getRate(), TimeUnit.MILLISECONDS);
	}

	@Override
	public void addListener(MonitorListener listener) {
		clusterRunnable.setListener(listener);
	}

	@Override
	public void saveMonitor(PlayOrmMonitor monitor) {
		MonitorDbo m = CopyUtil.copy(monitor);
		NoSqlEntityManager mgr = factory.createEntityManager();
		mgr.put(m);
		mgr.flush();
	}

	@Override
	public PlayOrmMonitor getMonitor(String id) {
		NoSqlEntityManager mgr = factory.createEntityManager();
		MonitorDbo mon = mgr.find(MonitorDbo.class, id);
		return CopyUtil.copy(mon);
	}

	public void setFactory(NoSqlEntityManagerFactory factory2) {
		this.factory = factory2;
	}

}
