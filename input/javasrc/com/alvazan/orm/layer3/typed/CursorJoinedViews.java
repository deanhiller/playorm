package com.alvazan.orm.layer3.typed;

import java.util.ArrayList;
import java.util.List;

import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

class CursorJoinedViews extends AbstractCursor<List<TypedRow>> {
	private List<DirectCursor<KeyValue<TypedRow>>> cursors;
	private List<ViewInfo> views;
	
	@Override
	public String toString() {
		return "CursorForTypeRowJoin["+cursors+"]";
	}

	public CursorJoinedViews(List<DirectCursor<KeyValue<TypedRow>>> cursors2, List<ViewInfo> views2) {
		this.cursors = cursors2;
		this.views = views2;
	}
	
	@Override
	public void beforeFirst() {
		for(DirectCursor<KeyValue<TypedRow>> d : cursors) {
			d.beforeFirst();
		}
	}

	@Override
	public Holder<List<TypedRow>> nextImpl() {
		boolean atLeastOneCursorHasNext = false;
		List<TypedRow> rows = new ArrayList<TypedRow>();
		for(int i = 0; i < cursors.size(); i++) {
			DirectCursor<KeyValue<TypedRow>> cursor = cursors.get(i);
			ViewInfo view = views.get(i);
			Holder<KeyValue<TypedRow>> next = cursor.nextImpl();
			if(next != null) {
				atLeastOneCursorHasNext = true;
				KeyValue<TypedRow> kv = next.getValue();
				TypedRow row = kv.getValue();
				if(row != null)
					row.setView(view);
				rows.add(row);
			} else {
				TypedRow row = new TypedRow(view, view.getTableMeta());
				rows.add(row);
			}
		}

		if(atLeastOneCursorHasNext)
			return new Holder<List<TypedRow>>(rows);
		return null;
	}
}