package com.alvazan.orm.api.base;

public class ToOneProvider<T> {

	private T inst;
	
	public T get() {
		return inst;
	}
	
	public void set(T inst) {
		this.inst = inst;
	}
}
