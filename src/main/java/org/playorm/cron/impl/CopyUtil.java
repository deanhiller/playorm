package org.playorm.cron.impl;

import org.playorm.cron.api.PlayOrmCronJob;
import org.playorm.cron.impl.db.MonitorDbo;

public class CopyUtil {

	public static MonitorDbo copy(PlayOrmCronJob monitor) {
		if(monitor == null)
			return null;
		MonitorDbo m = new MonitorDbo();
		m.setId(monitor.getId());
		m.setTimePeriodMillis(monitor.getTimePeriodMillis());
		m.setEpochOffset(monitor.getEpochOffset());
		
		String props = "";
		for(String key : monitor.getProperties().keySet()) {
			String value = monitor.getProperties().get(key);
			props += key+"="+value+"|";
		}
		m.setRawProperties(props);
		return m;
	}

	public static PlayOrmCronJob copy(MonitorDbo monitor) {
		if(monitor == null)
			return null;
		PlayOrmCronJob mon = new PlayOrmCronJob();
		mon.setId(monitor.getId());
		mon.setTimePeriodMillis(monitor.getTimePeriodMillis());
		mon.setEpochOffset(monitor.getEpochOffset());
		String props = monitor.getRawProperties();
		String[] propsArray = props.split("\\|");
		for(String prop : propsArray) {
			String[] kv = prop.split("=");
			mon.addProperty(kv[0], kv[1]);
		}
		return mon;
	}

}
