package com.alvazan.orm.api.base;

import com.alvazan.orm.api.z8spi.iter.Cursor;

public interface CursorToMany<T> extends Cursor<T> {

	void removeCurrent();

	void addElement(T element);

}
