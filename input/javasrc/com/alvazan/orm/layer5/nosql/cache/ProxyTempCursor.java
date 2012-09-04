package com.alvazan.orm.layer5.nosql.cache;

import java.util.Iterator;

import com.alvazan.orm.api.z8spi.AbstractCursor;

public class ProxyTempCursor<T> extends AbstractCursor<T> {

	private Iterator<T> iterator;
	private Iterable<T> iterable;

	public ProxyTempCursor(Iterable<T> iterable) {
		this.iterable = iterable;
		this.iterator = iterable.iterator();
	}

	@Override
	public com.alvazan.orm.api.z8spi.AbstractCursor.Holder<T> nextImpl() {
		if(!iterator.hasNext())
			return null;
		
		return new Holder<T>(iterator.next());
	}

	@Override
	public void beforeFirst() {
		iterator = iterable.iterator();
	}

}
