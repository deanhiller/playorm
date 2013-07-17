package com.alvazan.orm.layer0.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.alvazan.orm.impl.meta.data.MetaClass;

public class CursorRowPlain<T> extends AbstractCursor<T>{

	private static final Logger log = LoggerFactory.getLogger(CursorRowPlain.class);

	private MetaClass<T> meta;
	private AbstractCursor<Row> cursor;
	private NoSqlSession session;
	
	public CursorRowPlain(NoSqlSession session, MetaClass<T> meta2, AbstractCursor<Row> cursor) {
		this.session = session;
		this.meta = meta2;
		this.cursor = cursor;
	}

	@Override
	public void beforeFirst() {
		cursor.beforeFirst();
	}
	
	
	@Override
	public void afterLast() {
		cursor.afterLast();
	}
	
	@Override
	public Holder<T> nextImpl() {
		Holder<Row> nextImpl = cursor.nextImpl();
		if(nextImpl == null)
			return null;
		Row kv = nextImpl.getValue();
		T result = translateRow(kv);
		return new Holder<T>(result);
	}
	
	@Override
	public Holder<T> previousImpl() {
		Holder<Row> prevImpl = cursor.previousImpl();
		if(prevImpl == null)
			return null;
		Row kv = prevImpl.getValue();
		
		T result = translateRow(kv);
		return new Holder<T>(result);
	}
	
	private T translateRow(Row row) {
		KeyValue<T> keyVal = meta.translateFromRow(row, session);
		return keyVal.getValue();
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "CursorRow(rowToEntity)["+tabs+cursor+tabs+"]";
		StringLocal.set(tabs.length());
		return retVal;
	}
	
}
