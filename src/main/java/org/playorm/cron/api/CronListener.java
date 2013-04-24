package org.playorm.cron.api;

public interface CronListener {

	public void monitorFired(PlayOrmCronJob m);
}
