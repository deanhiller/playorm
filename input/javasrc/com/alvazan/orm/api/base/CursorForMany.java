package com.alvazan.orm.api.base;

import java.util.ArrayList;
import java.util.List;

import com.alvazan.orm.api.z8spi.iter.Cursor;

/**
 * This is used so you do not blow out your memory on a very wide row especially when you only want to display a few of the
 * results.
 * 
 * @author dhiller2
 *
 * @param <T>
 */
public class CursorForMany<T> implements Cursor<T> {

	private List<T> list = new ArrayList<T>();
	private int index;
	
	public CursorForMany() {
	}
	
	public CursorForMany(List<T> elements) {
		this.list.addAll(elements);
	}
	
	@Override
	public void beforeFirst() {
		index = 0;
	}

	@Override
	public boolean next() {
		index++;
		if(index < list.size())
			return true;
		return false;
	}

	@Override
	public T getCurrent() {
		return list.get(index);
	}

	public void removeCurrent() {
		list.remove(index);
		index--;
	}
	
	public void addElement(T element) {
		this.list.add(element);
	}
}
