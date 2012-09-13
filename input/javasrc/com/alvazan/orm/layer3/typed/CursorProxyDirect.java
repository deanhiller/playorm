package com.alvazan.orm.layer3.typed;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;

public class CursorProxyDirect implements Cursor<IndexColumnInfo> {

	private DirectCursor<IndexColumnInfo> iter;
	private Holder<IndexColumnInfo> cached;

	public CursorProxyDirect(DirectCursor<IndexColumnInfo> iter) {
		this.iter = iter;
	}

	@Override
	public void beforeFirst() {
		iter.beforeFirst();
	}

	@Override
	public boolean next() {
		cached = iter.nextImpl();
		if(cached == null)
			return false;
		return true;
	}

	@Override
	public IndexColumnInfo getCurrent() {
		if(cached == null)
			throw new IllegalArgumentException("You should test for true/false when calling next as you are not within the limits currently of the list of values");
		return cached.getValue();
	}

}
