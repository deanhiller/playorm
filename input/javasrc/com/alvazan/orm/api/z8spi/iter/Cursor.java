package com.alvazan.orm.api.z8spi.iter;

public interface Cursor<T> {

	/**
	 * Resets this Cursor to be be before the first element so you can call next again and start all over
	 */
	void beforeFirst();
	
	boolean next();

	T getCurrent();

}
