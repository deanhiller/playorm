package com.alvazan.orm.api.spi9.db;

import java.util.Collection;
import java.util.List;

import com.alvazan.orm.layer9z.spi.db.inmemory.RowImpl;
import com.google.inject.ImplementedBy;

@ImplementedBy(RowImpl.class)
public interface Row {

	byte[] getKey();

	void setKey(byte[] key);

	Collection<Column> getColumns();

	Column getColumn(byte[] key);

	void put(byte[] name, Column col);

	void remove(byte[] name);

	Collection<Column> columnSlice(byte[] from, byte[] to);
	
	Collection<IndexColumn> columnSlice(Key from, Key to);

	Collection<Column> columnByPrefix(byte[] prefix);

	void setColumns(List<Column> columns);

}
