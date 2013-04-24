package org.playorm.cron.test;

import org.playorm.cron.api.CronListener;
import org.playorm.cron.api.PlayOrmCronJob;

public class MockListener implements CronListener {

	private PlayOrmCronJob lastFired;

	@Override
	public void monitorFired(PlayOrmCronJob m) {
		this.lastFired = m;
	}

	public PlayOrmCronJob getLastFiredMonitor() {
		PlayOrmCronJob temp = lastFired;
		lastFired = null;
		return temp;
	}

}
