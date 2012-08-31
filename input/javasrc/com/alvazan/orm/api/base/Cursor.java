package com.alvazan.orm.api.base;

public interface Cursor<T> {

	boolean hasNext();

	T next();

}
