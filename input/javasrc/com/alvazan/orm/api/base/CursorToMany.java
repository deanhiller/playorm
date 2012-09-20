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

	/**
	 * When you call this method, it remove the current item AND moves back to previous item or points to just before the first item if at the beginning
	 * of the list.  This allows you to call cursor.next(), cursor.removeCurrent(), cursor.next(), cursor.removeCurrent repeatedly.
	 */
	void removeCurrent();

	/**
	 * Adds an element at the end of the cursor to be saved to the database.
	 * 
	 * @param element
	 */
	void addElement(T element);

}
