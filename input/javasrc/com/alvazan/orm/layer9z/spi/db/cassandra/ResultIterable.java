package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.Iterator;

import javax.inject.Provider;

import com.alvazan.orm.api.spi9.db.KeyValue;
import com.alvazan.orm.api.spi9.db.Row;
import com.netflix.astyanax.model.Rows;

public class ResultIterable implements Iterable<KeyValue<Row>> {

	private Rows<byte[], byte[]> rows;
	private Provider<Row> rowProvider;

	public ResultIterable(Provider<Row> rowProvider, Rows<byte[], byte[]> rows) {
		this.rowProvider = rowProvider;
		this.rows = rows;
	}

	@Override
	public Iterator<KeyValue<Row>> iterator() {
		return new ResultIteratorProxy(rowProvider, rows.iterator());
	}

	private static class ResultIteratorProxy implements Iterator<KeyValue<Row>> {

		private Iterator<com.netflix.astyanax.model.Row<byte[], byte[]>> rows;
		private Provider<Row> rowProvider;

		public ResultIteratorProxy(Provider<Row> rowProvider, Iterator<com.netflix.astyanax.model.Row<byte[], byte[]>> iterator) {
			this.rowProvider = rowProvider;
			this.rows = iterator;
		}

		@Override
		public boolean hasNext() {
			return rows.hasNext();
		}

		@Override
		public KeyValue<Row> next() {
			com.netflix.astyanax.model.Row<byte[], byte[]> row = rows.next();
			KeyValue<Row> kv = new KeyValue<Row>();
			kv.setKey(row.getKey());
			if(!row.getColumns().isEmpty()) {
				//Astyanax returns a row when there is none BUT we know if there are 0 columns there is really no row in the database
				//then
				Row r = rowProvider.get();
				r.setKey(row.getKey());
				CassandraSession.processColumns(row, r);
				kv.setValue(r);
			}
			
			return null;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported, never will be");
		}
	}
}
