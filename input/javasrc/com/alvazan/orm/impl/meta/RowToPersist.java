package com.alvazan.orm.impl.meta;

import java.util.ArrayList;
import java.util.List;

import com.alvazan.orm.layer3.spi.db.Column;

public class RowToPersist {
	private byte[] key;
	private List<Column> columns = new ArrayList<Column>();
	public byte[] getKey() {
		return key;
	}
	public void setKey(byte[] key) {
		this.key = key;
	}
	public List<Column> getColumns() {
		return columns;
	}
	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

}
