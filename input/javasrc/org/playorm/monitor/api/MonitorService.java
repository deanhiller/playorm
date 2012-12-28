package org.playorm.monitor.api;



public interface MonitorService {

	void start();

	void addListener(MonitorListener listener);

	void saveMonitor(PlayOrmMonitor monitor);

	PlayOrmMonitor getMonitor(String id);
	
}
