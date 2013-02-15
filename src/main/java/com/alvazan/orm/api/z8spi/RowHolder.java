package com.alvazan.orm.api.z8spi;

public class RowHolder<T> {

	private T value;
	private byte[] key;
	
	public RowHolder(byte[] key, T v) {
		this.key = key;
		this.value = v;
	}

	public RowHolder(byte[] key) {
		this.key = key;
	}

	public T getValue() {
		return value;
	}

	public byte[] getKey() {
		return key;
	}
	
}
