package com.alvazan.orm.layer2.nosql.cache;

public class RowHolder<T> {

	private T value;
	
	public RowHolder(T v) {
		this.value = v;
	}

	public T getValue() {
		return value;
	}
	
}
