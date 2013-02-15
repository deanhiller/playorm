package com.alvazan.orm.api.base;

import java.util.ArrayList;
import java.util.List;

/**
 * This is used so you do not blow out your memory on a very wide row especially when you only want to display a few of the
 * results.
 * 
 * @author dhiller2
 *
 * @param <T>
 */
public class CursorToManyImpl<T> implements CursorToMany<T> {

	private List<T> list = new ArrayList<T>();
	private int index;
	
	public CursorToManyImpl() {
	}
	
	public CursorToManyImpl(List<T> elements) {
		this.list.addAll(elements);
	}
	
	@Override
	public void beforeFirst() {
		index = -1;
	}
	
	@Override
	public void afterLast() {
		index = -1;
		if (list!=null)
			index=list.size();
	}

	@Override
	public boolean next() {
		index++;
		if(index < list.size())
			return true;
		return false;
	}
	
	@Override
	public boolean previous() {
		index--;
		if(index >= 0)
			return true;
		return false;
	}

	@Override
	public T getCurrent() {
		return list.get(index);
	}

	@Override
	public void removeCurrent() {
		list.remove(index);
		index--;
	}
	
	@Override
	public void addElement(T element) {
		this.list.add(element);
	}
	
	public List<T> getElementsToAdd() {
		return list;
	}
}
