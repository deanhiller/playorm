package com.alvazan.orm.layer5.query;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class CursorForPrimaryKey implements DirectCursor<IndexColumnInfo> {

	private AbstractCursor<KeyValue<Row>> cursor;
	private ViewInfo viewInfo;
	private DboColumnMeta colMeta;

	public CursorForPrimaryKey(ViewInfo viewInfo, DboColumnMeta info, AbstractCursor<KeyValue<Row>> scan) {
		Precondition.check(viewInfo, "viewInfo");
		Precondition.check(scan, "scan");
		this.viewInfo = viewInfo;
		this.cursor = scan;
		this.colMeta = info;
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "CursorForPrimaryKey["+
				tabs+cursor+
				tabs+viewInfo+
				tabs+"]";
		StringLocal.set(tabs.length());
		return retVal;
	}
	
	@Override
	public void beforeFirst() {
		cursor.beforeFirst();
	}

	@Override
	public Holder<IndexColumnInfo> nextImpl() {
		Holder<KeyValue<Row>> holder = cursor.nextImpl();
		if(holder == null)
			return null;
		KeyValue<Row> pkCol = holder.getValue();
		if(pkCol == null || pkCol.getValue() == null)
			return null;
		IndexColumn c = new IndexColumn();
		c.setPrimaryKey(colMeta.getOwner().getIdColumnMeta().unformVirtRowKey(pkCol.getValue().getKey()));
		c.setIndexedValue(colMeta.getOwner().getIdColumnMeta().unformVirtRowKey(pkCol.getValue().getKey()));
		c.setValue(pkCol.getValue().getKey());
		IndexColumnInfo info = new IndexColumnInfo();
		info.putIndexNode(viewInfo, c, colMeta);
		return new Holder<IndexColumnInfo>(info);
	}

}
