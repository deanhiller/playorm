package com.alvazan.orm.layer9z.spi.db.inmemory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.ByteArray;

public class IndexedRow extends RowImpl {
	
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
	public Collection<IndexColumn> columnSlice(Key from, Key to) {
		NavigableMap<OurKey, IndexColumn> resultMap = columns;
		if(from != null) {
			OurKey fromKey = new OurKey(from.getKey(), new byte[0]);
			resultMap = columns.tailMap(fromKey, from.isInclusive());
		}
		
		if(to != null) {
			OurKey toKey = new OurKey(to.getKey(), null);
			resultMap = resultMap.headMap(toKey, to.isInclusive());
		}
		
		List<IndexColumn> results = new ArrayList<IndexColumn>();
		for(IndexColumn c : resultMap.values()) {
			//HACK since I am not sure how to make this work and it is not too important...strip off
			//the ones that have the prefix passed in
			if(from != null && !from.isInclusive()) {
				if(isMatch(from, c))
					continue; //skip this value
			}
			if(to != null && !to.isInclusive()) {
				if(isMatch(to, c))
					continue; //skip this value since we are exclusive
			}
			
			results.add(c);
		}
		
		List<IndexColumn> cols = new ArrayList<IndexColumn>();
		for(IndexColumn c2 : results) {
			cols.add(c2.copy());
		}
		
		return cols;
	}

	private boolean isMatch(Key from, IndexColumn c) {
		boolean isMatch = false;
		ByteArray b = new ByteArray(from.getKey());
		ByteArray val = new ByteArray(c.getIndexedValue());
		if(b.equals(val))
			isMatch = true;
		return isMatch;
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
			if(primaryKey == null) {
				postfix = null;
				return;
			}
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
			
			int val = 0;
			if(postfix != null)
				val = postfix.hashCode();
			int val2 = 0;
			if(prefix != null)
				val2 = prefix.hashCode();
			
			result = prime * result + val;
			result = prime * result + val2;
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
