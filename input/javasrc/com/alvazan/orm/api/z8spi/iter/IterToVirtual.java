package com.alvazan.orm.api.z8spi.iter;

import java.util.Iterator;

import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;

public class IterToVirtual implements Iterable<byte[]> {

	private DboTableMeta meta;
	private Iterable<byte[]> noSqlKeys;

	public IterToVirtual(DboTableMeta meta, Iterable<byte[]> noSqlKeys) {
		this.meta = meta;
		this.noSqlKeys = noSqlKeys;
	}

	@Override
	public Iterator<byte[]> iterator() {
		return new IteratorToVirtual(meta, noSqlKeys.iterator());
	}
	
	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "IterToVirtual(keyToVirtualKeyTranslator)["+tabs+noSqlKeys+tabs+"]";
		StringLocal.set(tabs.length());
		return retVal;
	}
	
	private static class IteratorToVirtual implements Iterator<byte[]> {

		private Iterator<byte[]> iterator;
		private DboColumnIdMeta idMeta;

		public IteratorToVirtual(DboTableMeta meta, Iterator<byte[]> iterator) {
			this.idMeta = meta.getIdColumnMeta();
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public byte[] next() {
			byte[] next = iterator.next();
			return idMeta.formVirtRowKey(next);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported");
		}
	}
}
