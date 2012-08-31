package com.alvazan.orm.api.z8spi;

import java.util.Collection;
import java.util.List;

import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
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

	Row deepCopy();

	void addColumns(List<Column> columns);

	void removeColumns(Collection<byte[]> columnNames);

}
