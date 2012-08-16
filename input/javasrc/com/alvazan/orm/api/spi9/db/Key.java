package com.alvazan.orm.api.spi9.db;

public class Key {
	private byte[] key;
	private boolean inclusive;
	
	public Key() {};
	public Key(byte[] key, boolean inclusive) {
		this.key = key;
		this.inclusive = inclusive;
	}
	
	public byte[] getKey() {
		return key;
	}
	public void setKey(byte[] key) {
		this.key = key;
	}
	public boolean isInclusive() {
		return inclusive;
	}
	public void setInclusive(boolean inclusive) {
		this.inclusive = inclusive;
	}
	
	
}
