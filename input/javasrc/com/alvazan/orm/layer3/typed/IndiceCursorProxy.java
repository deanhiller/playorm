package com.alvazan.orm.layer3.typed;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z5api.IndexColumnInfo.Wrapper;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class IndiceCursorProxy implements DirectCursor<byte[]> {

	private DirectCursor<IndexColumnInfo> cursor;
	private ViewInfo view;
	
	public IndiceCursorProxy(ViewInfo view, DirectCursor<IndexColumnInfo> indice) {
		Precondition.check(view, "view");
		Precondition.check(indice, "indice");
		this.cursor = indice;
		this.view = view;
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "IndiceCursorProxy(proxyQueryCursorWithIterable)["
				+tabs+cursor
				+tabs+view
				+tabs+"]";
		StringLocal.set(tabs.length());
		return retVal;
	}
	
	
	@Override
	public Holder<byte[]> nextImpl() {
		Holder<IndexColumnInfo> next = cursor.nextImpl();
		if(next == null)
			return null;
		IndexColumnInfo info = next.getValue();
		Wrapper wrapper = info.getIndexNode(view);
		IndexColumn indNode = wrapper.getCol();
		byte[] key = indNode.getPrimaryKey();
		if(key == null)
			throw new IllegalArgumentException("key was null, index data seems corrupt on view="+view+" col="
					+wrapper.getColMeta()+" some value is tied to a null primary key.  Should" +
							" never happen even in face of eventual consistent, should never happen");
		return new Holder<byte[]>(key);
	}

	@Override
	public Holder<byte[]> previousImpl() {
		Holder<IndexColumnInfo> prev = cursor.previousImpl();
		if(prev == null)
			return null;
		IndexColumnInfo info = prev.getValue();
		Wrapper wrapper = info.getIndexNode(view);
		IndexColumn indNode = wrapper.getCol();
		byte[] key = indNode.getPrimaryKey();
		if(key == null)
			throw new IllegalArgumentException("key was null, index data seems corrupt on view="+view+" col="
					+wrapper.getColMeta()+" some value is tied to a null primary key.  Should" +
							" never happen even in face of eventual consistent, should never happen");
		return new Holder<byte[]>(key);
	}

	@Override
	public void beforeFirst() {
		cursor.beforeFirst();
	}

	@Override
	public void afterLast() {
		cursor.afterLast();
	}
}
