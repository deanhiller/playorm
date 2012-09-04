package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.Iterator;

import javax.inject.Provider;

import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.netflix.astyanax.model.Rows;

public class CursorResult extends AbstractCursor<KeyValue<Row>> {

	private Rows<byte[], byte[]> rowsIterable;
	private Provider<Row> rowProvider;

	private Iterator<com.netflix.astyanax.model.Row<byte[], byte[]>> rows;
	
	public CursorResult(Provider<Row> rowProvider, Rows<byte[], byte[]> rows) {
		this.rowProvider = rowProvider;
		this.rowsIterable = rows;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		rows = rowsIterable.iterator();
	}

	@Override
	public Holder<KeyValue<Row>> nextImpl() {
		if(!rows.hasNext())
			return null;
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
		
		return new Holder<KeyValue<Row>>(kv);		
	}
}
