package com.alvazan.orm.layer1.base;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import com.alvazan.orm.api.base.Index;
import com.alvazan.orm.api.base.KeyValue;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.spi.db.Row;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaIdField;
import com.alvazan.orm.impl.meta.data.MetaInfo;
import com.alvazan.orm.impl.meta.data.RowToPersist;

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
		if(metaClass == null)
			throw new IllegalArgumentException("Entity type="+entity.getClass().getName()+" was not scanned and added to meta information on startup.  It is either missing @NoSqlEntity annotation or it was not in list of scanned packages");
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
		for(int i = 0; i < rows.size(); i++) {
			Row row = rows.get(i);
			Object key = keys.get(i);
			
			KeyValue<T> keyVal;
			if(row == null) {
				keyVal = new KeyValue<T>();
				keyVal.setKey(key);
			} else {
				keyVal = meta.translateFromRow(row, session);
			}
			
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
		indexImpl.setup(metaClass, indexName, this, session);
		return indexImpl;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getReference(Class<T> entityType, Object key) {
		MetaClass<T> metaClass = metaInfo.getMetaClass(entityType);
		MetaIdField<T> field = metaClass.getIdField();
		return field.convertIdToProxy(session, key, null);
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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void fillInWithKey(Object entity) {
		MetaClass metaClass = metaInfo.getMetaClass(entity.getClass());
		MetaIdField idField = metaClass.getIdField();
		idField.fillInAndFetchId(entity);
	}

	@Override
	public void clearDbAndIndexesIfInMemoryType() {
		session.clearDbAndIndexesIfInMemoryType();
	}

}
