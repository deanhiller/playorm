package com.alvazan.orm.layer1.base;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import com.alvazan.orm.api.Index;
import com.alvazan.orm.api.KeyValue;
import com.alvazan.orm.api.NoSqlEntityManager;
import com.alvazan.orm.impl.meta.MetaClass;
import com.alvazan.orm.impl.meta.MetaIdField;
import com.alvazan.orm.impl.meta.MetaInfo;
import com.alvazan.orm.impl.meta.RowToPersist;
import com.alvazan.orm.layer2.nosql.NoSqlSession;
import com.alvazan.orm.layer2.nosql.Row;

public class BaseEntityManagerImpl implements NoSqlEntityManager {

	@Inject @Named("readcachelayer")
	private NoSqlSession session;
	@Inject
	private MetaInfo metaInfo;
	@SuppressWarnings("rawtypes")
	@Inject
	private Provider<IndexImpl> indexProvider; 
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void put(Object entity) {
		MetaClass metaClass = metaInfo.getMetaClass(entity.getClass());
		RowToPersist row = metaClass.translateToRow(entity);
		session.persist(metaClass.getColumnFamily(), row.getKey(), row.getColumns());
	}

	@Override
	public void putAll(List<Object> entities) {
		for(Object entity : entities) {
			put(entity);
		}
	}

	@Override
	public <T> T find(Class<T> entityType, Object key) {
		List<Object> keys = new ArrayList<Object>();
		keys.add(key);
		List<KeyValue<T>> entities = findAll(entityType, keys);
		return entities.get(0).getValue();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> List<KeyValue<T>> findAll(Class<T> entityType, List<Object> keys) {
		if(keys == null)
			throw new IllegalArgumentException("keys list cannot be null");
		MetaClass<T> meta = metaInfo.getMetaClass(entityType);
		List<byte[]> noSqlKeys = new ArrayList<byte[]>();
		for(Object k : keys) {
			byte[] key = meta.convertIdToNoSql(k);
			noSqlKeys.add(key);
		}
		
		//NOTE: It is WAY more efficient to find ALL keys at once then it is to
		//find one at a time.  You would rather have 1 find than 1000 if network latency was 1 ms ;).
		List<Row> rows = session.find(meta.getColumnFamily(), noSqlKeys);
		
		List<KeyValue<T>> keyValues = new ArrayList<KeyValue<T>>();
		for(Row row : rows) {
			KeyValue<T> keyVal = meta.translateFromRow(row, session);
			keyValues.add(keyVal);
		}
		
		return keyValues;
	}
	
	@Override
	public void flush() {
		session.flush();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public <T> Index<T> getIndex(Class<T> forEntity, String indexName) {
		MetaClass metaClass = metaInfo.getMetaClass(forEntity);
		IndexImpl indexImpl = indexProvider.get();
		indexImpl.setMeta(metaClass);
		indexImpl.setIndexName(indexName);
		indexImpl.setSession(session);
		return indexImpl;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getReference(Class<T> entityType, Object key) {
		MetaClass<T> metaClass = metaInfo.getMetaClass(entityType);
		MetaIdField<T> field = metaClass.getIdField();
		return field.convertIdToProxy(session, key);
	}

	@Override
	public NoSqlSession getSession() {
		return session;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Object getKey(Object entity) {
		MetaClass metaClass = metaInfo.getMetaClass(entity.getClass());
		return metaClass.fetchId(entity);
	}

}
