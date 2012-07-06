package com.alvazan.orm.layer1.base;

import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import com.alvazan.orm.api.Converter;
import com.alvazan.orm.api.Index;
import com.alvazan.orm.api.JoinInfo;
import com.alvazan.orm.api.Query;
import com.alvazan.orm.impl.meta.MetaClass;
import com.alvazan.orm.impl.meta.MetaIdField;
import com.alvazan.orm.impl.meta.MetaInfo;
import com.alvazan.orm.impl.meta.MetaQuery;
import com.alvazan.orm.layer2.nosql.NoSqlSession;
import com.alvazan.orm.layer3.spi.index.SpiIndexQuery;

public class IndexImpl<T> implements Index<T> {

	@Inject
	private MetaInfo metaInfo;
	@Inject
	private Provider<QueryAdapter<T>> adapterFactory;
	private MetaClass<T> metaClass;
	private String indexName;
	private NoSqlSession session;

	@Override
	public void addToIndex(T entity) {
		Map<String, String> item = metaClass.translateForIndex(entity);
		session.addToIndex(indexName, item);
	}

	@Override
	public void removeFromIndex(T entity) {
		Object id = metaClass.fetchId(entity);
		MetaIdField<T> idField = metaClass.getIdField();
		Converter converter = idField.getConverter();
		String indexId = converter.convertToIndexFormat(id);
		session.removeFromIndex(indexName, indexId);
	}

	@Override
	public Query<T> getNamedQuery(String name) {
		MetaQuery<T> metaQuery = metaClass.getNamedQuery(name);
		QueryAdapter<T> adapter = adapterFactory.get();
		SpiIndexQuery<T> indexQuery = metaQuery.createSpiAdapter(indexName);
		adapter.setup(metaQuery, indexQuery);
		return adapter;
	}

	public void setMeta(MetaClass<T> metaClass) {
		this.metaClass = metaClass;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public void setSession(NoSqlSession session) {
		this.session = session;
	}

	@Override
	public Query<T> getNamedQueryJoin(String name, JoinInfo... info) {
		throw new UnsupportedOperationException("We do not support joins just yet");
	}
}
