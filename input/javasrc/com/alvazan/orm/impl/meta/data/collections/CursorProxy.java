package com.alvazan.orm.impl.meta.data.collections;

import com.alvazan.orm.api.base.CursorToMany;
import com.alvazan.orm.api.z5api.NoSqlSession;

public class CursorProxy<T> implements CursorToMany<T> {

	private NoSqlSession session;
	private String colFamily;
	private String rowKey;

	public CursorProxy(NoSqlSession session, String colFamily, String rowKey) {
		this.session = session;
		this.colFamily = colFamily;
		this.rowKey = rowKey;
	}

	@Override
	public void beforeFirst() {
	}

	@Override
	public boolean next() {
		return false;
	}

	@Override
	public T getCurrent() {
		return null;
	}

	@Override
	public void removeCurrent() {
		throw new UnsupportedOperationException("very quick to add, let me know you used this...being nickled and dimed so trying to just get major stuff done");
	}

	@Override
	public void addElement(T element) {
		
	}

}
