package com.alvazan.orm.layer3.typed;

import java.util.Iterator;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.AbstractIterator;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class IterableCursorProxy implements Iterable<byte[]> {

	private DirectCursor<IndexColumnInfo> cursor;
	private ViewInfo view;
	
	public IterableCursorProxy(ViewInfo view, DirectCursor<IndexColumnInfo> indice) {
		Precondition.check(view, "view");
		Precondition.check(indice, "indice");
		this.cursor = indice;
		this.view = view;
	}

	@Override
	public Iterator<byte[]> iterator() {
		cursor.beforeFirst();
		return new IteratorCursorProxy(view, cursor);
	}
	
	private static class IteratorCursorProxy extends AbstractIterator<byte[]> {

		private DirectCursor<IndexColumnInfo> cursor;
		private ViewInfo view;

		public IteratorCursorProxy(ViewInfo view, DirectCursor<IndexColumnInfo> cursor2) {
			this.view = view;
			this.cursor = cursor2;
		}

		@Override
		public IterHolder<byte[]> nextImpl() {
			Holder<IndexColumnInfo> next = cursor.nextImpl();
			if(next == null)
				return null;
			IndexColumnInfo info = next.getValue();
			IndexColumn indNode = info.getIndexNode(view);
			byte[] key = indNode.getPrimaryKey();
			return new IterHolder<byte[]>(key);
		}
	}
}
