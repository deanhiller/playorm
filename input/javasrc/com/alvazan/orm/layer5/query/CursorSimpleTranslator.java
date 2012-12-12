package com.alvazan.orm.layer5.query;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class CursorSimpleTranslator implements DirectCursor<IndexColumnInfo> {

	private AbstractCursor<IndexColumn> cursor;
	private ViewInfo viewInfo;
	private DboColumnMeta colMeta;

	public CursorSimpleTranslator(ViewInfo viewInfo, DboColumnMeta info, AbstractCursor<IndexColumn> scan) {
		Precondition.check(viewInfo, "viewInfo");
		Precondition.check(scan, "scan");
		this.viewInfo = viewInfo;
		this.cursor = scan;
		this.colMeta = info;
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "CursorSimpleTranslator["+tabs+cursor+tabs+"view="+viewInfo+tabs+"]";
		StringLocal.set(tabs.length());
		return retVal;
	}
	
	@Override
	public void beforeFirst() {
		cursor.beforeFirst();
	}

	@Override
	public Holder<IndexColumnInfo> nextImpl() {
		Holder<IndexColumn> holder = cursor.nextImpl();
		if(holder == null)
			return null;
		IndexColumn indCol = holder.getValue();
		if(indCol == null)
			return new Holder<IndexColumnInfo>(null);
		IndexColumnInfo info = new IndexColumnInfo();
		info.putIndexNode(viewInfo, indCol, colMeta);
		return new Holder<IndexColumnInfo>(info);
	}

}
