package com.alvazan.orm.api.base;

public interface Cursor<T> {

	/**
	 * Resets this Cursor to be be before the first element so you can call next again and start all over
	 */
	void beforeFirst();
	
	boolean hasNext();

	T next();

}
