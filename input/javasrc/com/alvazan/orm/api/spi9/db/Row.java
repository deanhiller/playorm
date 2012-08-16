package com.alvazan.orm.api.spi9.db;

import java.util.Collection;


public interface Row {

	byte[] getKey();

	void setKey(byte[] key);

	Collection<Column> getColumns();

	Column getColumn(byte[] key);

	void put(byte[] name, Column col);

	void remove(byte[] name);

	Collection<Column> columnSlice(byte[] from, byte[] to);

	Collection<Column> columnByPrefix(byte[] prefix);

	Collection<Column> columnRangeScanAll();
	
}
