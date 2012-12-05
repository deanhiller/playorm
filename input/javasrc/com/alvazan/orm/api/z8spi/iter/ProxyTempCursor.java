package com.alvazan.orm.api.z8spi.iter;

import java.util.Iterator;



public class ProxyTempCursor<T> extends AbstractCursor<T> {

	private Iterable<T> iterable;
	private Iterator<T> iterator;

	public ProxyTempCursor(Iterable<T> iterable) {
		this.iterable = iterable;
		beforeFirst();
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "ProxyTempCursor["+tabs+iterable+tabs+"]";
		StringLocal.set(tabs.length());
		return retVal;
	}
	
	@Override
	public void beforeFirst() {
		iterator = iterable.iterator();
	}
	
	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<T> nextImpl() {
		if(!iterator.hasNext())
			return null;
		return new Holder<T>(iterator.next());
	}

}
