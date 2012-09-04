package com.alvazan.orm.util;

import java.util.Iterator;

import com.alvazan.orm.api.z8spi.AbstractCursor;

public class ProxyTempCursor<T> extends AbstractCursor<T> {

	private Iterable<T> iterable;
	private Iterator<T> iterator;

	public ProxyTempCursor(Iterable<T> iterable) {
		this.iterable = iterable;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		iterator = iterable.iterator();
	}
	
	@Override
	public com.alvazan.orm.api.z8spi.AbstractCursor.Holder<T> nextImpl() {
		if(!iterator.hasNext())
			return null;
		return new Holder<T>(iterator.next());
	}

}
