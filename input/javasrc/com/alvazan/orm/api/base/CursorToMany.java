package com.alvazan.orm.api.base;

import com.alvazan.orm.api.z8spi.iter.Cursor;

/**
 * For situations on very wide rows where you don't want to run out of memory, you can use a CursorToMany.
 * 
 * @author dhiller2
 *
 * @param <T>
 */
public interface CursorToMany<T> extends Cursor<T> {

	void removeCurrent();

	void addElement(T element);

}
