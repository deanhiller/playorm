package com.alvazan.orm.layer1.base;

import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import com.alvazan.orm.api.base.Index;
import com.alvazan.orm.api.base.JoinInfo;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.base.exc.PkIsNullException;
import com.alvazan.orm.api.spi.index.SpiQueryAdapter;
import com.alvazan.orm.api.spi.layer2.Converter;
import com.alvazan.orm.api.spi.layer2.MetaQuery;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaIdField;
import com.alvazan.orm.impl.meta.data.MetaInfo;

public class IndexImpl<T> implements Index<T> {

	@SuppressWarnings("rawtypes")
	@Inject
	private Provider<QueryAdapter> adapterFactory;
	@Inject
	private MetaInfo metaInfo;
	private MetaClass<T> metaClass;
	private String indexName;
	private NoSqlSession session;
	private BaseEntityManagerImpl entityMgr;

	@Override
	public void addToIndex(T entity) {
		Map<String, Object> item = metaClass.translateForIndex(entity);
		String id = (String) item.get(MetaClass.IDKEY);
		session.addToIndex(indexName, id, item);
	}

	@Override
	public void removeFromIndex(T entity) {
		Object id = metaClass.fetchId(entity);
		MetaIdField<T> idField = metaClass.getIdField();
		Converter converter = idField.getConverter();
		String indexId = converter.convertToIndexFormat(id);
		if(indexId == null)
			throw new PkIsNullException("entity="+entity+" has no pk defined, so we fail " +
					"now before you called flush so nothing has been added to database nor " +
					"the index(as long as you haven't called flush yet");		
		
		session.removeFromIndex(indexName, indexId);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Query<T> getNamedQuery(String name) {
		MetaQuery<T> metaQuery = metaClass.getNamedQuery(name);
		SpiQueryAdapter spiAdapter = metaQuery.createSpiMetaQuery(indexName);
		
		//We cannot return MetaQuery since it is used by all QueryAdapters and each QueryAdapter
		//runs in a different thread potentially while MetaQuery is one used by all threads
		QueryAdapter<T> adapter = adapterFactory.get();
		adapter.setup(metaQuery, spiAdapter, entityMgr, metaClass);
		return adapter;
	}

	@Override
	public Query<T> getNamedQueryJoin(String name, JoinInfo... info) {
		throw new UnsupportedOperationException("We do not support joins just yet");
	}

	public void setup(MetaClass<T> metaClass2, String indexName2,
			BaseEntityManagerImpl entityMgr, NoSqlSession session2) {
		this.metaClass = metaClass2;
		this.indexName = indexName2;
		this.entityMgr = entityMgr;
		this.session = session2;
	}
}
