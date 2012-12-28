package com.alvazan.orm.api.z8spi.iter;

import java.util.Iterator;

import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.layer3.typed.IndiceCursorProxy;

public class IterableWrappingCursor<T> implements DirectCursor<T> {

	private Iterable<T> iter;
	private Iterator<T> currentIterator;

	public IterableWrappingCursor(Iterable<T> iter) {
		this.iter = iter;
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "IteratorWrappingCursor(iteratorWrappingCursor)["+tabs+iter+tabs+"]";
		StringLocal.set(tabs.length());
		return retVal;
	}
	

	@Override
	public Holder<T> nextImpl() {
		if (currentIterator == null)
			beforeFirst();
		if (!currentIterator.hasNext())
			return null;
		return new Holder<T>(currentIterator.next());
	}

	@Override
	public Holder<T> previousImpl() {
		throw new UnsupportedOperationException("We can't go backward on a cursor that is wrapping an iterator");
	}

	@Override
	public void beforeFirst() {
		currentIterator = iter.iterator();
		
	}

	@Override
	public void afterLast() {
		throw new UnsupportedOperationException("We can't go backward on a cursor that is wrapping an iterator");
	}
}
