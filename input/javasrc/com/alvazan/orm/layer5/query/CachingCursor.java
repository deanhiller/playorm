package com.alvazan.orm.layer5.query;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;

public class CachingCursor<T> implements DirectCursor<IndexColumnInfo> {

	private DirectCursor<IndexColumnInfo> cursor;

	public CachingCursor(DirectCursor<IndexColumnInfo> cursor) {
		this.cursor = cursor;
	}

	@Override
	public Holder<IndexColumnInfo> nextImpl() {
		return cursor.nextImpl();
	}

	@Override
	public void beforeFirst() {
		cursor.beforeFirst();
	}

}
