package com.alvazan.orm.api.spi5;

public class KeyTuple {
	private Object key;
	private byte[] noSqlKey;
	public Object getKey() {
		return key;
	}
	public void setKey(Object key) {
		this.key = key;
	}
	public byte[] getNoSqlKey() {
		return noSqlKey;
	}
	public void setNoSqlKey(byte[] noSqlKey) {
		this.noSqlKey = noSqlKey;
	}
}