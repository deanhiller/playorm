package com.alvazan.orm.api.spi3;

import com.alvazan.orm.api.exc.RowNotFoundException;


public class KeyValue<T> {

	private Object key;
	private T value;
	private RowNotFoundException e;
	
	public Object getKey() {
		if(e != null)
			throw new RowNotFoundException("Row was referenced in index, but not found in nosql store.  key="+key, e);
		return key;
	}
	public void setKey(Object key) {
		this.key = key;
	}
	public T getValue() {
		if(e != null)
			throw new RowNotFoundException("Row was referenced in index, but not found in nosql store.  key="+key, e);
		return value;
	}
	public void setValue(T value) {
		this.value = value;
	}
	public void setException(RowNotFoundException exc) {
		this.e = exc;
	}
	public RowNotFoundException getException() {
		return e;
	}
}
