package com.alvazan.orm.layer9z.spi.db.cassandra;

import com.alvazan.orm.api.base.Cursor;
import com.alvazan.orm.api.z8spi.action.IndexColumn;

public class EmptyCursor<T> implements Cursor<T> {

	@Override
	public void beforeFirst() {
	}

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public T next() {
		throw new IllegalArgumentException("You should really call hasNext first, this cursor is out of items(actually it never had any so hah!, serves you right)");
	}

}
