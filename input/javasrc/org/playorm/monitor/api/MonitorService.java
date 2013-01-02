package org.playorm.monitor.api;

import java.util.List;



public interface MonitorService {

	void start();

	void addListener(MonitorListener listener);

	void saveMonitor(PlayOrmMonitor monitor);

	PlayOrmMonitor getMonitor(String id);
	
	List<PlayOrmMonitor> getMonitors(List<String> ids);
	
}
