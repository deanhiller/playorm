package com.alvazan.orm.layer9z.spi.db.inmemory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.ByteArray;

public class RowImpl implements Row {
	private byte[] key;
	private NavigableMap<ByteArray, Column> columns = new TreeMap<ByteArray, Column>();
	
	public RowImpl() {
	}

	public RowImpl(TreeMap<ByteArray, Column> map) {
		this.columns = map;
	}
	
	
	@Override
	public String toString() {
		return "rowKey="+new ByteArray(key)+" columns="+columns;
	}

	/* (non-Javadoc)
	 * @see com.alvazan.orm.layer3.spi.db.inmemory.RowTemp#getKey()
	 */
	@Override
	public byte[] getKey() {
		return key;
	}
	/* (non-Javadoc)
	 * @see com.alvazan.orm.layer3.spi.db.inmemory.RowTemp#setKey(byte[])
	 */
	@Override
	public void setKey(byte[] key) {
		this.key = key;
	}
	
	/* (non-Javadoc)
	 * @see com.alvazan.orm.layer3.spi.db.inmemory.RowTemp#getColumns()
	 */
	@Override
	public Collection<Column> getColumns() {
		return columns.values();
	}

	/* (non-Javadoc)
	 * @see com.alvazan.orm.layer3.spi.db.inmemory.RowTemp#getColumn(byte[])
	 */
	@Override
	public Column getColumn(byte[] key) {
		ByteArray bKey = new ByteArray(key);
		return columns.get(bKey);
	}
	/* (non-Javadoc)
	 * @see com.alvazan.orm.layer3.spi.db.inmemory.RowTemp#put(byte[], com.alvazan.orm.api.spi3.db.Column)
	 */
	@Override
	public void put(Column col) {
		ByteArray key = new ByteArray(col.getName());
		columns.put(key, col);
	}
	/* (non-Javadoc)
	 * @see com.alvazan.orm.layer3.spi.db.inmemory.RowTemp#remove(byte[])
	 */
	@Override
	public void remove(byte[] name) {
		ByteArray key = new ByteArray(name);
		columns.remove(key);
	}
	
	/* (non-Javadoc)
	 * @see com.alvazan.orm.layer3.spi.db.inmemory.RowTemp#columnSlice(byte[], byte[])
	 */
	@Override
	public Collection<IndexColumn> columnSlice(Key from, Key to) {
		throw new UnsupportedOperationException("bug, this is not an index row");
	}
	public Collection<Column> columnSlice(byte[] from, byte[] to) {
		NavigableMap<ByteArray, Column> map = columns;
		if(from != null) {
			ByteArray fromArray = new ByteArray(from);
			map = columns.tailMap(fromArray, true);
		}
		
		if(to != null) {
			ByteArray toArray = new ByteArray(to);
			map = map.headMap(toArray, true);
		}

		List<Column> list = deepCopy(map.values());
		
		return list;
	}

	private List<Column> deepCopy(Collection<Column> map) {
		List<Column> list = new ArrayList<Column>();
		for(Column c : map) {
			list.add(c.copy());
		}
		return list;
	}

	/* (non-Javadoc)
	 * @see com.alvazan.orm.layer3.spi.db.inmemory.RowTemp#columnByPrefix(byte[])
	 */
	@Override
	public Collection<Column> columnByPrefix(byte[] prefix) {
		List<Column> prefixed = new ArrayList<Column>();
		boolean started = false;
		for(Entry<ByteArray, Column> col : columns.entrySet()) {
			if(col.getKey().hasPrefix(prefix)) {
				started = true;
				prefixed.add(col.getValue());
			} else if(started)
				break; //since we hit the prefix and we are sorted, we can break.
		}
		return deepCopy(prefixed);
	}

	@Override
	public void addColumns(List<Column> cols) {
		for(Column c : cols) {
			ByteArray b = new ByteArray(c.getName());
			columns.put(b, c);
		}
	}

	@Override
	public Row deepCopy() {
		RowImpl impl = new RowImpl();
		impl.key = key;
		for(Entry<ByteArray, Column> s : columns.entrySet()) {
			impl.columns.put(s.getKey(), s.getValue().copy());
		}
		return impl;
	}

	@Override
	public void removeColumns(Collection<byte[]> columnNames) {
		for(byte[] k : columnNames) {
			ByteArray b = new ByteArray(k);
			columns.remove(b);
		}
	}

}
