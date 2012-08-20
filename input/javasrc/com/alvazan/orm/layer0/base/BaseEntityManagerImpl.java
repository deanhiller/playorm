package com.alvazan.orm.layer0.base;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Partition;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.orm.api.spi3.NoSqlTypedSession;
import com.alvazan.orm.api.spi3.meta.DboColumnIdMeta;
import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.DboDatabaseMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.api.spi3.meta.IndexData;
import com.alvazan.orm.api.spi3.meta.RowToPersist;
import com.alvazan.orm.api.spi3.meta.StorageTypeEnum;
import com.alvazan.orm.api.spi3.meta.conv.Converter;
import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi9.db.Column;
import com.alvazan.orm.api.spi9.db.KeyValue;
import com.alvazan.orm.api.spi9.db.Row;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaIdField;
import com.alvazan.orm.impl.meta.data.MetaInfo;
import com.alvazan.orm.impl.meta.data.NoSqlProxy;
import com.alvazan.orm.layer3.typed.NoSqlTypedSessionImpl;

public class BaseEntityManagerImpl implements NoSqlEntityManager {

	@Inject @Named("readcachelayer")
	private NoSqlSession session;
	@Inject
	private MetaInfo metaInfo;
	@SuppressWarnings("rawtypes")
	@Inject
	private Provider<PartitionImpl> indexProvider; 
	@Inject
	private NoSqlTypedSessionImpl typedSession;
	@Inject
	private DboDatabaseMeta databaseInfo;
	
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

		String cf = metaClass.getColumnFamily();
		//NOW for index removals if any indexed values change of the entity, we remove from the index
		for(IndexData ind : row.getIndexToRemove()) {
			session.removeFromIndex(cf, ind.getColumnFamilyName(), ind.getRowKeyBytes(), ind.getIndexColumn());
		}
		
		//NOW for index adds, if it is a new entity or if values change, we persist those values
		for(IndexData ind : row.getIndexToAdd()) {
			session.persistIndex(cf, ind.getColumnFamilyName(), ind.getRowKeyBytes(), ind.getIndexColumn());
		}

		byte[] key = row.getKey();
		List<Column> cols = row.getColumns();
		session.put(cf, key, cols);
	}

//	@Override
//	public void putAll(List<Object> entities) {
//		for(Object entity : entities) {
//			put(entity);
//		}
//	}

	@Override
	public <T> T find(Class<T> entityType, Object key) {
		List<Object> keys = new ArrayList<Object>();
		keys.add(key);
		Iterable<KeyValue<T>> entities = findAll(entityType, keys);
		return entities.iterator().next().getValue();
	}
	
	@Override
	public <T> Iterable<KeyValue<T>> findAll(Class<T> entityType, Iterable<? extends Object> keys) {
		if(keys == null)
			throw new IllegalArgumentException("keys list cannot be null");
		MetaClass<T> meta = metaInfo.getMetaClass(entityType);
		if(meta == null)
			throw new IllegalArgumentException("Class type="+entityType.getName()+" was not found, please check that you scanned the right package and look at the logs to see if this class was scanned");

		Iterable<byte[]> iter = new IterProxy<T>(meta, keys);
		
		return findAllImpl2(meta, iter, null);
	}
	
	<T> Iterable<KeyValue<T>> findAllImpl2(MetaClass<T> meta, Iterable<byte[]> noSqlKeys, String query) {
		//NOTE: It is WAY more efficient to find ALL keys at once then it is to
		//find one at a time.  You would rather have 1 find than 1000 if network latency was 1 ms ;).
		String cf = meta.getColumnFamily();
		Iterable<KeyValue<Row>> rows = session.findAll(cf, noSqlKeys);
		return new IterRowProxy<T>(meta, rows, session, query);
	}

	@SuppressWarnings("unchecked")
	public <T> List<KeyValue<T>> findAllList(Class<T> entityType, List<? extends Object> keys) {
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
		Iterable<KeyValue<Row>> rows2 = session.findAll(cf, noSqlKeys);
		List<Row> rows = new ArrayList<Row>();
		for(KeyValue<Row> kv : rows2) {
			rows.add(kv.getValue());
		}
		
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

	@SuppressWarnings("unchecked")
	@Override
	public <T> Partition<T> getPartition(Class<T> forEntity, String tableColumnName, Object partitionObj) {
		MetaClass<T> metaClass = metaInfo.getMetaClass(forEntity);
		PartitionImpl<T> indexImpl = indexProvider.get();
		indexImpl.setup(metaClass, tableColumnName, partitionObj, this, session);
		return indexImpl;
	}
	
	@Override
	public <T> Query<T> createNamedQuery(Class<T> forEntity, String namedQuery) {
		Partition<T> indexImpl = getPartition(forEntity, null, null);
		return indexImpl.createNamedQuery(namedQuery);		
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
	public void clearDatabase(boolean recreateMeta) {
		session.clearDb();
		
		saveMetaData();
	}

	void saveMetaData() {
		BaseEntityManagerImpl tempMgr = this;
        DboDatabaseMeta existing = tempMgr.find(DboDatabaseMeta.class, DboDatabaseMeta.META_DB_ROWKEY);
//        if(existing != null)
//        	throw new IllegalStateException("Your property NoSqlEntityManagerFactory.AUTO_CREATE_KEY is set to 'create' which only creates meta data if none exist already but meta already exists");
		
        for(DboTableMeta table : databaseInfo.getAllTables()) {
        	
        	for(DboColumnMeta col : table.getAllColumns()) {
        		tempMgr.put(col);
        	}
        	
        	tempMgr.put(table.getIdColumnMeta());
        	
        	tempMgr.put(table);
        }
        
        databaseInfo.setId(DboDatabaseMeta.META_DB_ROWKEY);
        
        //NOW, on top of the ORM entites, we have 3 special index column families of String, BigInteger and BigDecimal
        //which are one of the types in the composite column name.(the row keys are all strings).  The column names
        //are <value being indexed of String or BigInteger or BigDecimal><primarykey><length of first value> so we can
        //sort it BUT we can determine the length of first value so we can get to primary key.
        
        for(StorageTypeEnum type : StorageTypeEnum.values()) {
        	if(type == StorageTypeEnum.BYTES)
        		continue;
        	DboTableMeta cf = new DboTableMeta();
        	cf.setColumnFamily(type.getIndexTableName());
        	cf.setColNamePrefixType(type);
        	
        	DboColumnIdMeta idMeta = new DboColumnIdMeta();
        	idMeta.setup(cf, "id", String.class, false);
        	
        	tempMgr.put(idMeta);
        	tempMgr.put(cf);
        	
        	databaseInfo.addMetaClassDbo(cf);
        }
        
        tempMgr.put(databaseInfo);
        tempMgr.flush();
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
		
		String cf = metaClass.getColumnFamily();
		//REMOVE EVERYTHING HERE, we are probably removing extra and could optimize this later
		for(IndexData ind : indexToRemove) {
			session.removeFromIndex(cf, ind.getColumnFamilyName(), ind.getRowKeyBytes(), ind.getIndexColumn());
		}
		
		session.remove(cf, rowKey);
	}

	@Override
	public NoSqlTypedSession getTypedSession() {
		if(!isTypedSessionInitialized) {
			typedSession.setInformation(session);
		}
		return typedSession;
	}

}
