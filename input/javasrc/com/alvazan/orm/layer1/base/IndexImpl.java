package com.alvazan.orm.layer1.base;

import com.alvazan.orm.api.Index;
import com.alvazan.orm.api.Query;
import com.alvazan.orm.impl.meta.MetaClass;

public class IndexImpl<T> implements Index<T> {

	private MetaClass metaClass;
	private String indexName;

	@Override
	public void addToIndex(T entity) {
	}

	@Override
	public void removeFromIndex(T entity) {
	}

	@Override
	public Query<T> getNamedQuery(String name) {
		return null;
	}

	public void setMeta(MetaClass metaClass) {
		this.metaClass = metaClass;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

}
