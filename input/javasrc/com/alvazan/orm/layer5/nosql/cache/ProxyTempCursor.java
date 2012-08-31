package com.alvazan.orm.layer5.nosql.cache;

import java.util.Iterator;

import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.util.AbstractCursor;

public class ProxyTempCursor extends AbstractCursor<IndexColumn> {

	private Iterator<IndexColumn> iterator;
	private Iterable<IndexColumn> iterable;

	public ProxyTempCursor(Iterable<IndexColumn> iterable) {
		this.iterable = iterable;
		this.iterator = iterable.iterator();
	}

	@Override
	protected com.alvazan.orm.util.AbstractCursor.Holder<IndexColumn> nextImpl() {
		if(!iterator.hasNext())
			return null;
		
		return new Holder<IndexColumn>(iterator.next());
	}

	@Override
	public void beforeFirst() {
		iterator = iterable.iterator();
	}

}
