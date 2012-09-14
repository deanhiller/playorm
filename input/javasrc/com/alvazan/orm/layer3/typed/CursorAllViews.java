package com.alvazan.orm.layer3.typed;

import java.util.List;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class CursorAllViews extends AbstractCursor<List<TypedRow>> {

	private List<ViewInfo> views;
	private DirectCursor<IndexColumnInfo> cursor;

	public CursorAllViews(List<ViewInfo> views, DirectCursor<IndexColumnInfo> directCursor) {
		this.views = views;
		this.cursor = directCursor;
	}

	@Override
	public void beforeFirst() {
		cursor.beforeFirst();
	}

	@Override
	public Holder<List<TypedRow>> nextImpl() {
		Holder<IndexColumnInfo> next = cursor.nextImpl();
		if(next == null)
			return null;
		
		IndexColumnInfo index = next.getValue();
		
		return null;
	}

}
