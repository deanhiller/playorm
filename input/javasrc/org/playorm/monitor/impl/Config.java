package org.playorm.monitor.impl;

public class Config {

	private long rate;
	private String hostName;

	public Config(long rate, String host) {
		this.rate = rate;
		this.hostName = host;
	}

	public long getRate() {
		return rate;
	}

	public String getHostName() {
		return hostName;
	}
	
}
