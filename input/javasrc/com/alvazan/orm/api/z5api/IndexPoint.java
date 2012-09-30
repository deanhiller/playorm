package com.alvazan.orm.api.z5api;

import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;

public class IndexPoint {

	private DboColumnIdMeta idMeta;
	private DboColumnMeta colMeta;
	private byte[] rowKey;
	private byte[] indexedValue;

	public IndexPoint(DboColumnIdMeta idMeta, IndexColumn col, DboColumnMeta colMeta) {
		this.idMeta = idMeta;
		this.colMeta = colMeta;
		rowKey = col.getPrimaryKey();
		indexedValue = col.getIndexedValue();
	}

	@Override
	public String toString() {
		return getIndexedValueAsString()+"."+getKeyAsString();
	}

	public Object getKey() {
		return idMeta.convertFromStorage2(rowKey);
	}
	public String getKeyAsString() {
		Object obj = getKey();
		return idMeta.convertTypeToString(obj);
	}
	
	public byte[] getRawKey() {
		return rowKey;
	}

	public byte[] getRawIndexedValue() {
		return indexedValue;
	}
	
	public Object getIndexedValue() {
		return colMeta.convertFromStorage2(indexedValue);
	}
	public String getIndexedValueAsString() {
		Object obj = getIndexedValue();
		return colMeta.convertTypeToString(obj);
	}

	public DboColumnMeta getColumnMeta() {
		return colMeta;
	}

	public DboColumnIdMeta getRowKeyMeta() {
		return idMeta;
	}
}
