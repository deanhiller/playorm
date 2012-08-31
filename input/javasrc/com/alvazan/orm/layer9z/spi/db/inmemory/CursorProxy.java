package com.alvazan.orm.layer9z.spi.db.inmemory;

import java.util.Iterator;

import com.alvazan.orm.util.AbstractCursor;

public class CursorProxy<T> extends AbstractCursor<T> {

	private Iterable<T> iterable;
	private Iterator<T> iterator;

	public CursorProxy(Iterable<T> iter) {
		this.iterable = iter;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		iterator = iterable.iterator();
	}

	@Override
	protected com.alvazan.orm.util.AbstractCursor.Holder<T> nextImpl() {
		if(!iterator.hasNext())
			return null;
		return new Holder<T>(iterator.next());
	}

}
