package com.alvazan.orm.layer0.base;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;

public class DebugCursor implements DirectCursor<IndexColumnInfo> {

	private DirectCursor<IndexColumnInfo> cursor;
	private boolean alreadyRan;

	public DebugCursor(DirectCursor<IndexColumnInfo> cursor) {
		this.cursor = cursor;
	}

	@Override
	public Holder<IndexColumnInfo> nextImpl() {
		return cursor.nextImpl();
	}
	
	@Override
	public Holder<IndexColumnInfo> previousImpl() {
		return cursor.previousImpl();
	}

	@Override
	public void beforeFirst() {
		if(alreadyRan)
			throw new IllegalArgumentException("This cursor cannot be reset");
		alreadyRan = true;
	}
	
	@Override
	public void afterLast() {
		if(alreadyRan)
			throw new IllegalArgumentException("This cursor cannot be reset");
		alreadyRan = true;
	}

}
