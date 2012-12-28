package com.alvazan.orm.api.z8spi.iter;

import java.util.List;
import java.util.ListIterator;

import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;

public class ListWrappingCursor<T> implements DirectCursor<T> {

	private List<T> backingList;
	private ListIterator<T> backingListIterator;

	public ListWrappingCursor(List<T> list) {
		backingList = list;
		this.backingListIterator = list.listIterator();
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "ListWrappingCursor(ListWrappingCursor)["+tabs+backingListIterator+tabs+"]";
		StringLocal.set(tabs.length());
		return retVal;
	}
	

	@Override
	public Holder<T> nextImpl() {
		if (backingListIterator.hasNext())
			return new Holder<T>(backingListIterator.next());
		return null;
	}

	@Override
	public Holder<T> previousImpl() {
		if (backingListIterator.hasPrevious())
			return new Holder<T>(backingListIterator.previous());
		return null;
	}

	@Override
	public void beforeFirst() {
		backingListIterator = backingList.listIterator();
	}

	@Override
	public void afterLast() {
		while (backingListIterator.hasNext())
			backingListIterator.next();
	}
}
