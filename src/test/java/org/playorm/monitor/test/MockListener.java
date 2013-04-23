package org.playorm.monitor.test;

import org.playorm.cron.api.MonitorListener;
import org.playorm.cron.api.PlayOrmMonitor;

public class MockListener implements MonitorListener {

	private PlayOrmMonitor lastFired;

	@Override
	public void monitorFired(PlayOrmMonitor m) {
		this.lastFired = m;
	}

	public PlayOrmMonitor getLastFiredMonitor() {
		PlayOrmMonitor temp = lastFired;
		lastFired = null;
		return temp;
	}

}
