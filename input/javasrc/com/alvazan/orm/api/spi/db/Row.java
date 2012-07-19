package com.alvazan.orm.api.spi.db;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class Row {
	private byte[] key;
	private Map<ByteArray, Column> columns = new HashMap<ByteArray, Column>();
	public byte[] getKey() {
		return key;
	}
	public void setKey(byte[] key) {
		this.key = key;
	}
	
	public Collection<Column> getColumns() {
		return columns.values();
	}

	public Column getColumn(byte[] key) {
		return columns.get(new ByteArray(key));
	}
	public void put(byte[] name, Column col) {
		columns.put(new ByteArray(name), col);
	}
	public void remove(byte[] name) {
		columns.remove(new ByteArray(name));
	}
	
	private static class ByteArray {
		private byte[] key;
		public ByteArray(byte[] key) {
			this.key = key;
		}
		@Override
		public int hashCode() {
			long hash = 0;
			for(int i = 0; i < key.length;i++) {
				hash += key[i];
			}
			
			return (int) (hash / 2);
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ByteArray other = (ByteArray) obj;
			if (!Arrays.equals(key, other.key))
				return false;
			return true;
		}
		
	}

}
