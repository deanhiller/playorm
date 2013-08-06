package com.alvazan.orm.api.z8spi.action;



public class PersistIndex extends RemoveIndex implements Action {
	private int ttl;

	public void setRowTtl(int rowTTL) {
		this.ttl = rowTTL;
	}

	public int getRowTtl() {
		return ttl;
	}
}
