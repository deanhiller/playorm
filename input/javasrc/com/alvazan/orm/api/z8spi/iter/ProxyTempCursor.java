package com.alvazan.orm.api.z8spi.iter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.ListIterator;



public class ProxyTempCursor<T> extends AbstractCursor<T> {

	private ListIterator<T> items;

	public ProxyTempCursor(Collection<T> items) {
		this.items=new ArrayList<T>(items).listIterator();
		beforeFirst();
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "ProxyTempCursor["+tabs+items+tabs+"]";
		StringLocal.set(tabs.length());
		return retVal;
	}
	
	@Override
	public void beforeFirst() {
		while(items.hasPrevious()) items.previous();
	}
	
	@Override
	public void afterLast() {
		while(items.hasNext()) items.next();
	}
	
	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<T> nextImpl() {
		if(!items.hasNext())
			return null;
		return new Holder<T>(items.next());
	}
	
	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<T> previousImpl() {
		if(!items.hasPrevious())
			return null;
		return new Holder<T>(items.previous());
	}

}
