package com.alvazan.orm.layer5.nosql.cache;

public class RowHolder<T> {

	private T value;
	private byte[] key;
	
	public RowHolder(byte[] key, T v) {
		this.key = key;
		this.value = v;
	}

	public T getValue() {
		return value;
	}

	public byte[] getKey() {
		return key;
	}
	
}
