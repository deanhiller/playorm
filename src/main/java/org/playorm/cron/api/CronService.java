package org.playorm.cron.api;

import java.util.List;



public interface CronService {

	void start();

	void addListener(CronListener listener);

	
	void saveMonitor(PlayOrmCronJob monitor);

	PlayOrmCronJob getMonitor(String id);
	
	List<PlayOrmCronJob> getMonitors(List<String> ids);

	void deleteMonitor(String id);
}
