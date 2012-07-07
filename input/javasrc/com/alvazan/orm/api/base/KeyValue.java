package com.alvazan.orm.api.base;

public class KeyValue<T> {

	private Object key;
	private T value;
	public Object getKey() {
		return key;
	}
	public void setKey(Object key) {
		this.key = key;
	}
	public T getValue() {
		return value;
	}
	public void setValue(T value) {
		this.value = value;
	}
}
