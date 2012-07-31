package com.alvazan.orm.api.spi3.index;

import com.alvazan.orm.api.spi2.DboColumnMeta;

public class ValAndType {

	private byte[] indexedData;
	private DboColumnMeta columnMeta;
	public byte[] getIndexedData() {
		return indexedData;
	}
	public void setIndexedData(byte[] indexedData) {
		this.indexedData = indexedData;
	}
	public DboColumnMeta getColumnMeta() {
		return columnMeta;
	}
	public void setColumnMeta(DboColumnMeta columnMeta) {
		this.columnMeta = columnMeta;
	}
	
}
