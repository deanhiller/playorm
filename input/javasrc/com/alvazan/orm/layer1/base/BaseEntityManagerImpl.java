package com.alvazan.orm.layer1.base;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import com.alvazan.orm.api.base.Index;
import com.alvazan.orm.api.base.KeyValue;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.ColumnType;
import com.alvazan.orm.api.spi3.db.IndexColumn;
import com.alvazan.orm.api.spi3.db.Row;
import com.alvazan.orm.impl.meta.data.IndexData;
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
		
		//This is if we need to be removing columns from the row that represents the entity in a oneToMany or ManyToMany
		//as the entity.accounts may have removed one of the accounts!!!
		if(row.hasRemoves())
			session.remove(metaClass.getColumnFamily(), row.getKey(), row.getColumnNamesToRemove());
		
		//NOW for index removals if any indexed values change of the entity, we remove from the index
		for(IndexData ind : row.getIndexToRemove()) {
			//ColumnType type = ind.getIndexedValueType().translateStoreToColumnType();
			session.removeFromIndex(ind.getColumnFamilyName(), ind.getRowKeyBytes(), ind.getIndexColumn());
		}
		
		//NOW for index adds, if it is a new entity or if values change, we persist those values
		for(IndexData ind : row.getIndexToAdd()) {
			session.persistIndex(ind.getColumnFamilyName(), ind.getRowKeyBytes(), ind.getIndexColumn());
		}
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
	public <T> List<KeyValue<T>> findAll(Class<T> entityType, List<? extends Object> keys) {
		if(keys == null)
			throw new IllegalArgumentException("keys list cannot be null");
		MetaClass<T> meta = metaInfo.getMetaClass(entityType);
		if(meta == null)
			throw new IllegalArgumentException("Class type="+entityType.getName()+" was not found, please check that you scanned the right package and look at the logs to see if this class was scanned");
		List<byte[]> noSqlKeys = new ArrayList<byte[]>();
		for(Object k : keys) {
			byte[] key = meta.convertIdToNoSql(k);
			noSqlKeys.add(key);
		}
		
		return findAllImpl(meta, keys, noSqlKeys);
	}

	<T> List<KeyValue<T>> findAllImpl(MetaClass<T> meta, List<? extends Object> keys, List<byte[]> noSqlKeys) {
		//NOTE: It is WAY more efficient to find ALL keys at once then it is to
		//find one at a time.  You would rather have 1 find than 1000 if network latency was 1 ms ;).
		String cf = meta.getColumnFamily();
		List<Row> rows = session.find(cf, noSqlKeys);	
		return getKeyValues( meta,keys,rows);
	}
	
	private <T> List<KeyValue<T>> getKeyValues(MetaClass<T> meta,List<? extends Object> keys,List<Row> rows){
		List<KeyValue<T>> keyValues = new ArrayList<KeyValue<T>>();

		for(int i = 0; i < rows.size(); i++) {
			Row row = rows.get(i);
			Object key;
			if(keys != null)
				key = keys.get(i);
			else
				key = meta.getIdField().getConverter().convertFromNoSql(row.getKey());
			
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

	@Override
	public <T> Index<T> getIndex(Class<T> forEntity) {
		MetaClass metaClass = metaInfo.getMetaClass(forEntity);
		IndexImpl indexImpl = indexProvider.get();
		indexImpl.setup(metaClass, "/"+metaClass.getColumnFamily(), this, session);
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

	public void setup() {
		session.setOrmSessionForMeta(this);		
	}

	@Override
	public void remove(Object entity) {
		throw new UnsupportedOperationException("not done yet");
	}

}
