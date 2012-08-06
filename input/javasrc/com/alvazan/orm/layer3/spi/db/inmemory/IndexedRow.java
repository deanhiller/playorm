package com.alvazan.orm.layer3.spi.db.inmemory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.alvazan.orm.api.spi3.db.ByteArray;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.IndexColumn;
import com.alvazan.orm.api.spi3.db.Row;

public class IndexedRow extends Row {
	
	private NavigableMap<OurKey, IndexColumn> columns = new TreeMap<OurKey, IndexColumn>();
	
	public IndexedRow(TreeMap<OurKey, IndexColumn> map) {
		this.columns = map;
	}

	public void addIndexedColumn(IndexColumn column) {
		OurKey key = new OurKey(column.getIndexedValue(), column.getPrimaryKey());
		columns.put(key, column);
	}
	public void removeIndexedColumn(IndexColumn column) {
		OurKey key = new OurKey(column.getIndexedValue(), column.getPrimaryKey());
		columns.remove(key);
	}
	
	
	
	@Override
	public Collection<Column> columnSlice(byte[] from, byte[] to) {
		OurKey fromKey = new OurKey(from, new byte[0]);
		OurKey toKey = new OurKey(to, new byte[0]);
		NavigableMap<OurKey, IndexColumn> resultMap = columns.subMap(fromKey, true, toKey, true);
		
		List<Column> results = new ArrayList<Column>();
		for(IndexColumn c : resultMap.values()) {
			Column col = new Column();
			col.setName(c.getPrimaryKey());
		}
		return results;
	}

	@Override
	public Collection<Column> columnByPrefix(byte[] prefix) {
		throw new UnsupportedOperationException("bug, I don't think this should be called but could be wrong");
	}



	public static class OurKey {
		private ByteArray prefix;
		private ByteArray postfix;

		public OurKey(byte[] indexedValue, byte[] primaryKey) {
			prefix = new ByteArray(indexedValue);
			postfix = new ByteArray(primaryKey);
		}

		
		public ByteArray getPrefix() {
			return prefix;
		}


		public ByteArray getPostfix() {
			return postfix;
		}


		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((postfix == null) ? 0 : postfix.hashCode());
			result = prime * result
					+ ((prefix == null) ? 0 : prefix.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			OurKey other = (OurKey) obj;
			if (postfix == null) {
				if (other.postfix != null)
					return false;
			} else if (!postfix.equals(other.postfix))
				return false;
			if (prefix == null) {
				if (other.prefix != null)
					return false;
			} else if (!prefix.equals(other.prefix))
				return false;
			return true;
		}
	}

}
