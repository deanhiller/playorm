package org.playorm.monitor.api;

import java.util.HashMap;
import java.util.Map;

public class PlayOrmMonitor {

	private String id;
	
	private long timePeriodMillis;
	
	private Map<String, String> properties = new HashMap<String, String>();

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public long getTimePeriodMillis() {
		return timePeriodMillis;
	}

	public void setTimePeriodMillis(long timePeriodMillis) {
		this.timePeriodMillis = timePeriodMillis;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public void addProperty(String name, String value) {
		properties.put(name, value);
	}
	
	public void removeProperty(String name) {
		properties.remove(name);
	}
	
}
