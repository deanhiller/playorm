package com.alvazan.test;

import com.alvazan.orm.api.base.DbTypeEnum;

public class Config {

	private DbTypeEnum serverType;
	private String clusterName;
	private String seeds;

	public Config(DbTypeEnum serverType, String clusterName, String seeds) {
		this.serverType = serverType;
		this.clusterName = clusterName;
		this.seeds = seeds;
	}

	public DbTypeEnum getServerType() {
		return serverType;
	}

	public void setServerType(DbTypeEnum serverType) {
		this.serverType = serverType;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getSeeds() {
		return seeds;
	}

	public void setSeeds(String seeds) {
		this.seeds = seeds;
	}
}
