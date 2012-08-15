package com.alvazan.orm.layer0.base;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import com.alvazan.orm.api.base.Partition;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.orm.api.spi1.KeyValue;
import com.alvazan.orm.api.spi1.NoSqlTypedSession;
import com.alvazan.orm.api.spi1.meta.IndexData;
import com.alvazan.orm.api.spi1.meta.RowToPersist;
import com.alvazan.orm.api.spi1.meta.conv.Converter;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.Row;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaIdField;
import com.alvazan.orm.impl.meta.data.MetaInfo;
import com.alvazan.orm.impl.meta.data.NoSqlProxy;

public class BaseEntityManagerImpl implements NoSqlEntityManager {

	@Inject @Named("readcachelayer")
	private NoSqlSession session;
	@Inject
	private MetaInfo metaInfo;
	@SuppressWarnings("rawtypes")
	@Inject
	private Provider<IndexImpl> indexProvider; 
	@Inject
	private NoSqlTypedSession typedSession;
	private boolean isTypedSessionInitialized = false;
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void put(Object entity) {
		Class cl = entity.getClass();
		MetaClass metaClass = metaInfo.getMetaClass(cl);
		if(metaClass == null)
			throw new IllegalArgumentException("Entity type="+entity.getClass().getName()+" was not scanned and added to meta information on startup.  It is either missing @NoSqlEntity annotation or it was not in list of scanned packages");
		
		RowToPersist row = metaClass.translateToRow(entity);
		
		//This is if we need to be removing columns from the row that represents the entity in a oneToMany or ManyToMany
		//as the entity.accounts may have removed one of the accounts!!!
		if(row.hasRemoves())
			session.remove(metaClass.getColumnFamily(), row.getKey(), row.getColumnNamesToRemove());
		
		//NOW for index removals if any indexed values change of the entity, we remove from the index
		for(IndexData ind : row.getIndexToRemove()) {
			session.removeFromIndex(ind.getColumnFamilyName(), ind.getRowKeyBytes(), ind.getIndexColumn());
		}
		
		//NOW for index adds, if it is a new entity or if values change, we persist those values
		for(IndexData ind : row.getIndexToAdd()) {
			session.persistIndex(ind.getColumnFamilyName(), ind.getRowKeyBytes(), ind.getIndexColumn());
		}
		String cf = metaClass.getColumnFamily();
		byte[] key = row.getKey();
		List<Column> cols = row.getColumns();
		session.put(cf, key, cols);
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
		
		return findAllImpl(meta, keys, noSqlKeys, null);
	}

	<T> List<KeyValue<T>> findAllImpl(MetaClass<T> meta, List<? extends Object> keys, List<byte[]> noSqlKeys, String indexName) {
		//NOTE: It is WAY more efficient to find ALL keys at once then it is to
		//find one at a time.  You would rather have 1 find than 1000 if network latency was 1 ms ;).
		String cf = meta.getColumnFamily();
		List<Row> rows = session.find(cf, noSqlKeys);
		return getKeyValues(meta, keys, noSqlKeys, rows, indexName);
	}
	
	private <T> List<KeyValue<T>> getKeyValues(MetaClass<T> meta,List<? extends Object> keys,List<byte[]> noSqlKeys,List<Row> rows, String indexName){
		List<KeyValue<T>> keyValues = new ArrayList<KeyValue<T>>();

		if(keys != null)
			translateRows(meta, keys, rows, keyValues);
		else
			translateRowsForQuery(meta, noSqlKeys, rows, keyValues, indexName);
		
		return keyValues;
	}

	private <T> void translateRowsForQuery(MetaClass<T> meta, List<byte[]> noSqlKeys, List<Row> rows, List<KeyValue<T>> keyValues, String indexName) {
		for(int i = 0; i < rows.size(); i++) {
			Row row = rows.get(i);
			byte[] rowKey = noSqlKeys.get(i);
			MetaIdField<T> idField = meta.getIdField();
			Converter converter = idField.getConverter();
			Object key = converter.convertFromNoSql(rowKey);
			
			KeyValue<T> keyVal;
			if(row == null) {
				keyVal = new KeyValue<T>();
				keyVal.setKey(key);
				RowNotFoundException exc = new RowNotFoundException("Your query="+indexName+" contained a value with a pk where that entity no longer exists in the nosql store");
				keyVal.setException(exc);
			} else {
				keyVal = meta.translateFromRow(row, session);
			}
			
			keyValues.add(keyVal);
		}		
	}
	
	private <T> void translateRows(MetaClass<T> meta,
			List<? extends Object> keys, List<Row> rows,
			List<KeyValue<T>> keyValues) {
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
	}
	
	@Override
	public void flush() {
		session.flush();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public <T> Partition<T> getIndex(Class<T> forEntity, String indexName) {
		MetaClass metaClass = metaInfo.getMetaClass(forEntity);
		IndexImpl indexImpl = indexProvider.get();
		indexImpl.setup(metaClass, null, null, this, session);
		return indexImpl;
	}
	@SuppressWarnings("unchecked")
	@Override
	public <T> Partition<T> getPartition(Class<T> forEntity, String tableColumnName,
			Object partitionObj) {
		MetaClass<T> metaClass = metaInfo.getMetaClass(forEntity);
		IndexImpl<T> indexImpl = indexProvider.get();
		indexImpl.setup(metaClass, tableColumnName, partitionObj, this, session);
		return indexImpl;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> Query<T> createNamedQuery(Class<T> forEntity, String namedQuery) {
		MetaClass<T> metaClass = metaInfo.getMetaClass(forEntity);
		IndexImpl<T> indexImpl = indexProvider.get();
		indexImpl.setup(metaClass, null, null, this, session);
		return indexImpl.getNamedQuery(namedQuery);		
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
	public void clearDatabase() {
		session.clearDb();
	}

	public void setup() {
		session.setOrmSessionForMeta(this);		
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void remove(Object entity) {
		MetaClass metaClass = metaInfo.getMetaClass(entity.getClass());
		if(metaClass == null)
			throw new IllegalArgumentException("Entity type="+entity.getClass().getName()+" was not scanned and added to meta information on startup.  It is either missing @NoSqlEntity annotation or it was not in list of scanned packages");

		Object proxy = entity;
		Object pk = metaClass.fetchId(entity);
		byte[] rowKey = metaClass.convertIdToNoSql(pk);
			
		if(!metaClass.hasIndexedField()) {
			session.remove(metaClass.getColumnFamily(), rowKey);
			return;
		} else if(!(entity instanceof NoSqlProxy)) {
			//then we don't have the database information for indexes so we need to read from the database
			proxy = find(metaClass.getMetaClass(), pk);
		}
		
		List<IndexData> indexToRemove = metaClass.findIndexRemoves((NoSqlProxy)proxy, rowKey);
		
		//REMOVE EVERYTHING HERE, we are probably removing extra and could optimize this later
		for(IndexData ind : indexToRemove) {
			session.removeFromIndex(ind.getColumnFamilyName(), ind.getRowKeyBytes(), ind.getIndexColumn());
		}
		
		session.remove(metaClass.getColumnFamily(), rowKey);
	}

	@Override
	public NoSqlTypedSession getTypedSession() {
		if(!isTypedSessionInitialized) {
			typedSession.setInformation(session, this);
		}
		return typedSession;
	}

}
