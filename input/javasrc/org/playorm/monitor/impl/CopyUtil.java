package org.playorm.monitor.impl;

import org.playorm.monitor.api.PlayOrmMonitor;
import org.playorm.monitor.impl.db.MonitorDbo;

public class CopyUtil {

	public static MonitorDbo copy(PlayOrmMonitor monitor) {
		MonitorDbo m = new MonitorDbo();
		m.setId(monitor.getId());
		m.setTimePeriodMillis(monitor.getTimePeriodMillis());
		
		String props = "";
		for(String key : monitor.getProperties().keySet()) {
			String value = monitor.getProperties().get(key);
			props += key+"="+value+"|";
		}
		m.setRawProperties(props);
		return m;
	}

	public static PlayOrmMonitor copy(MonitorDbo monitor) {
		PlayOrmMonitor mon = new PlayOrmMonitor();
		mon.setId(monitor.getId());
		mon.setTimePeriodMillis(monitor.getTimePeriodMillis());
		String props = monitor.getRawProperties();
		String[] propsArray = props.split("\\|");
		for(String prop : propsArray) {
			String[] kv = prop.split("=");
			mon.addProperty(kv[0], kv[1]);
		}
		return mon;
	}

}
