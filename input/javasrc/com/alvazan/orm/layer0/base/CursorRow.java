package com.alvazan.orm.layer0.base;

import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.impl.meta.data.MetaClass;

public class CursorRow<T> extends AbstractCursor<KeyValue<T>>{

	private MetaClass<T> meta;
	private String query;
	private AbstractCursor<KeyValue<Row>> cursor;
	private NoSqlSession session;
	
	public CursorRow(NoSqlSession session, MetaClass<T> meta2, AbstractCursor<KeyValue<Row>> cursor,
			String query2) {
		this.session = session;
		this.meta = meta2;
		this.cursor = cursor;
	}

	@Override
	public void beforeFirst() {
		cursor.beforeFirst();
	}
	
	@Override
	public Holder<KeyValue<T>> nextImpl() {
		Holder<KeyValue<Row>> nextImpl = cursor.nextImpl();
		if(nextImpl == null)
			return null;
		KeyValue<Row> kv = nextImpl.getValue();
		
		KeyValue<T> result = translateRow(kv);
		return new Holder<KeyValue<T>>(result);
	}
	
	private KeyValue<T> translateRow(KeyValue<Row> kv) {
		Row row = kv.getValue();
		Object key = kv.getKey();
		
		KeyValue<T> keyVal;
		if(row == null) {
			keyVal = new KeyValue<T>();
			Object obj = meta.getIdField().translateFromBytes((byte[]) key);
			if(query != null) {
				RowNotFoundException exc = new RowNotFoundException("Your query="+query+" contained a value with a pk where that entity no longer exists in the nosql store");
				keyVal.setException(exc);
			}
			keyVal.setKey(obj);
		} else {
			keyVal = meta.translateFromRow(row, session);
		}
		
		return keyVal;
	}
	
}
