package com.alvazan.orm.layer3.typed;

import java.util.Iterator;
import java.util.List;

import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class CursorFillNulls implements DirectCursor<KeyValue<TypedRow>> {

	private DirectCursor<KeyValue<TypedRow>> cursor;
	private List<byte[]> fullKeyList;
	private Iterator<byte[]> keyIter;
	private ViewInfo view;

	public CursorFillNulls(DirectCursor<KeyValue<TypedRow>> cursor,
			List<byte[]> fullKeyList, ViewInfo view) {
		this.cursor = cursor;
		this.fullKeyList = fullKeyList;
		this.view = view;
		keyIter = fullKeyList.iterator();
	}

	@Override
	public Holder<KeyValue<TypedRow>> nextImpl() {
		if(!keyIter.hasNext())
			return null;
		
		byte[] theKey = keyIter.next();
		if(theKey == null) {
			KeyValue<TypedRow> kv = new KeyValue<TypedRow>();
			TypedRow row = new TypedRow(view, view.getTableMeta());
			kv.setValue(row);
			return new Holder<KeyValue<TypedRow>>(kv);
		}

		return cursor.nextImpl();
	}
	
	@Override
	public Holder<KeyValue<TypedRow>> previousImpl() {
		if(!keyIter.hasNext())
			return null;
		
		byte[] theKey = keyIter.next();
		if(theKey == null) {
			KeyValue<TypedRow> kv = new KeyValue<TypedRow>();
			TypedRow row = new TypedRow(view, view.getTableMeta());
			kv.setValue(row);
			return new Holder<KeyValue<TypedRow>>(kv);
		}

		return cursor.nextImpl();
	}

	@Override
	public void beforeFirst() {
		cursor.beforeFirst();
		keyIter = fullKeyList.iterator();
	}
	
	@Override
	public void afterLast() {
		cursor.afterLast();
		keyIter = fullKeyList.iterator();
	}

}
