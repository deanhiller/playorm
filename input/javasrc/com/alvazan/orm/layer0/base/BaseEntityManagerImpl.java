package com.alvazan.orm.layer0.base;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.MetaLayer;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.Query;
import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z5api.SpiMetaQuery;
import com.alvazan.orm.api.z5api.SpiQueryAdapter;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.MetaLoader;
import com.alvazan.orm.api.z8spi.MetaLookup;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.iter.IndiceToVirtual;
import com.alvazan.orm.api.z8spi.iter.IterToVirtual;
import com.alvazan.orm.api.z8spi.iter.IterableWrappingCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboDatabaseMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.RowToPersist;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaIdField;
import com.alvazan.orm.impl.meta.data.MetaInfo;
import com.alvazan.orm.impl.meta.data.NoSqlProxy;
import com.alvazan.orm.layer3.typed.IndiceCursorProxy;
import com.alvazan.orm.layer3.typed.NoSqlTypedSessionImpl;

public class BaseEntityManagerImpl implements NoSqlEntityManager, MetaLookup, MetaLoader {

	private static final Logger log = LoggerFactory.getLogger(BaseEntityManagerImpl.class);
	
	@Inject @Named("readcachelayer")
	private NoSqlSession session;
	@Inject
	private MetaInfo metaInfo;
	@SuppressWarnings("rawtypes")
	@Inject
	private Provider<QueryAdapter> adapterFactory;
	@Inject
	private NoSqlTypedSessionImpl typedSession;
	@Inject
	private DboDatabaseMeta databaseInfo;
	@Inject
	private MetaLayerImpl metaImpl;
	
	private boolean isTypedSessionInitialized = false;
	
	@SuppressWarnings("rawtypes")
	public void put(Object entity, boolean isInsert) {
		boolean needRead = false;
		if(!isInsert && !(entity instanceof NoSqlProxy)) {
			if(log.isDebugEnabled())
				log.debug("need read as isInsert="+isInsert);
			needRead = true;
		}
		
		Class cl = entity.getClass();
		MetaClass metaClass = metaInfo.getMetaClass(cl);
		if(metaClass == null)
			throw new IllegalArgumentException("Entity type="+entity.getClass().getName()+" was not scanned and added to meta information on startup.  It is either missing @NoSqlEntity annotation or it was not in list of scanned packages");
		
		putImpl(entity, needRead, metaClass);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void put(Object entity) {
		Class cl = entity.getClass();
		MetaClass metaClass = metaInfo.getMetaClass(cl);
		if(metaClass == null)
			throw new IllegalArgumentException("Entity type="+entity.getClass().getName()+" was not scanned and added to meta information on startup.  It is either missing @NoSqlEntity annotation or it was not in list of scanned packages");

		boolean needRead = false;
		MetaIdField idField = metaClass.getIdField();
		Object id = metaClass.fetchId(entity);
		if(idField.isAutoGen() && id != null && !(entity instanceof NoSqlProxy)) {
			//We do NOT have the data from the database IF this is NOT a NoSqlProxy and we need it since this is an 
			//update
			needRead = true;
		}
		putImpl(entity, needRead, metaClass);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void putImpl(Object originalEntity, boolean needRead, MetaClass metaClass) {
		Object entity = originalEntity;
		if(needRead) {
			Object id = metaClass.fetchId(originalEntity);
			Object temp = find(metaClass.getMetaClass(), id);
			if(log.isDebugEnabled())
				log.debug("entity with id="+id+" is="+temp);
			if(temp != null) {
				entity = temp;
				BeanProps.copyProps(originalEntity, entity);
			}
		}
		
		RowToPersist row = metaClass.translateToRow(entity);
		
		DboTableMeta metaDbo = metaClass.getMetaDbo();
		if (metaDbo.isEmbeddable())
			throw new IllegalArgumentException("Entity type="+entity.getClass().getName()+" can not be saved as it is Embedded Entity. And Embeddable entitites are only saved along with their parent entity");
		//This is if we need to be removing columns from the row that represents the entity in a oneToMany or ManyToMany
		//as the entity.accounts may have removed one of the accounts!!!
		if(row.hasRemoves())
			session.remove(metaDbo, row.getKey(), row.getColumnNamesToRemove());

		//NOW for index removals if any indexed values change of the entity, we remove from the index
		for(IndexData ind : row.getIndexToRemove()) {
			session.removeFromIndex(metaDbo, ind.getColumnFamilyName(), ind.getRowKeyBytes(), ind.getIndexColumn());
		}
		
		//NOW for index adds, if it is a new entity or if values change, we persist those values
		for(IndexData ind : row.getIndexToAdd()) {
			session.persistIndex(metaDbo, ind.getColumnFamilyName(), ind.getRowKeyBytes(), ind.getIndexColumn());
		}

		byte[] virtKey = row.getVirtualKey();
		List<Column> cols = row.getColumns();
		session.put(metaDbo, virtKey, cols);
	}

	@Override
	public <T> T find(Class<T> entityType, Object key) {
		if(key == null)
			throw new IllegalArgumentException("key must be supplied but was null");
		List<Object> keys = new ArrayList<Object>();
		keys.add(key);
		Cursor<KeyValue<T>> entities = findAll(entityType, keys);
		entities.next();
		return entities.getCurrent().getValue();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> Cursor<KeyValue<T>> findAll(Class<T> entityType, Iterable<? extends Object> keys) {
		if(keys == null)
			throw new IllegalArgumentException("keys list cannot be null");
		MetaClass<T> meta = metaInfo.getMetaClass(entityType);
		if(meta == null)
			throw new IllegalArgumentException("Class type="+entityType.getName()+" was not found, please check that you scanned the right package and look at the logs to see if this class was scanned");

		Iterable<byte[]> iter = new IterableKey<T>(meta, keys);
		Iterable<byte[]> virtKeys = new IterToVirtual(meta.getMetaDbo(), iter);
		
		//we pass in null for batch size such that we do infinite size or basically all keys passed into this method in one
		//shot
		return findAllImpl2(meta, new IterableWrappingCursor<byte[]>(virtKeys), null, true, null);
	}
	
	<T> AbstractCursor<KeyValue<T>> findAllImpl2(MetaClass<T> meta, ViewInfo mainView, DirectCursor<IndexColumnInfo> keys, String query, boolean cacheResults, Integer batchSize) {
		//OKAY, so this gets interesting.  The noSqlKeys could be a proxy iterable to 
		//millions of keys with some batch size.  We canNOT do a find inline here but must do the find in
		//batches as well
		IndiceCursorProxy indiceCursor = new IndiceCursorProxy(mainView, keys);
		DirectCursor<byte[]> virtKeys = new IndiceToVirtual(meta.getMetaDbo(), indiceCursor);
		return findAllImpl2(meta, virtKeys, query, cacheResults, batchSize);
	}
	
	
	<T> AbstractCursor<KeyValue<T>> findAllImpl2(MetaClass<T> meta, DirectCursor<byte[]> keys, String query, boolean cacheResults, Integer batchSize) {
		boolean skipCache = query != null;
		AbstractCursor<KeyValue<Row>> cursor = session.find(meta.getMetaDbo(), keys, skipCache, cacheResults, batchSize);
		return new CursorRow<T>(session, meta, cursor, query);
	}

	@SuppressWarnings("unchecked")
	public <T> List<KeyValue<T>> findAllList(Class<T> entityType, List<? extends Object> keys) {
		if(keys == null)
			throw new IllegalArgumentException("keys list cannot be null");
		MetaClass<T> meta = metaInfo.getMetaClass(entityType);
		if(meta == null)
			throw new IllegalArgumentException("Class type="+entityType.getName()+" was not found, please check that you scanned the right package and look at the logs to see if this class was scanned");
		
		List<KeyValue<T>> all = new ArrayList<KeyValue<T>>();
		Cursor<KeyValue<T>> results = findAll(entityType, keys);
		while(results.next()) {
			KeyValue<T> r = results.getCurrent();
			all.add(r);
		}
		
		return all;
	}
	
	@Override
	public void flush() {
		session.flush();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Query<T> createNamedQuery(Class<T> forEntity, String namedQuery) {
		MetaClass<T> metaClass = metaInfo.getMetaClass(forEntity);
		SpiMetaQuery metaQuery = metaClass.getNamedQuery(forEntity, namedQuery);
		
		SpiQueryAdapter spiAdapter = metaQuery.createQueryInstanceFromQuery(session);
		
		//We cannot return MetaQuery since it is used by all QueryAdapters and each QueryAdapter
		//runs in a different thread potentially while MetaQuery is one used by all threads
		QueryAdapter<T> adapter = adapterFactory.get();
		adapter.setup(metaClass, metaQuery, spiAdapter, this, forEntity);
		return adapter;
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

	@Override
	public MetaLayer getMeta() {
		return metaImpl;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void fillInWithKey(Object entity) {
		MetaClass metaClass = metaInfo.getMetaClass(entity.getClass());
		MetaIdField idField = metaClass.getIdField();
		if (idField != null)
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
			if (!table.isEmbeddable())
				tempMgr.put(table.getIdColumnMeta());
        	
        	tempMgr.put(table);
        }
        
        databaseInfo.setId(DboDatabaseMeta.META_DB_ROWKEY);
        
        //NOW, on top of the ORM entites, we have 3 special index column families of String, BigInteger and BigDecimal
        //which are one of the types in the composite column name.(the row keys are all strings).  The column names
        //are <value being indexed of String or BigInteger or BigDecimal><primarykey><length of first value> so we can
        //sort it BUT we can determine the length of first value so we can get to primary key.
        
        for(StorageTypeEnum type : StorageTypeEnum.values()) {
        	//Do we want a byte[] index that could be used with == but not < and not >, etc. etc.
        	//Do we want a separate Boolean index????  I don't think so
        	if(type != StorageTypeEnum.DECIMAL
        			&& type != StorageTypeEnum.INTEGER
        			&& type != StorageTypeEnum.STRING)
        		continue;
        	
        	DboTableMeta cf = new DboTableMeta();

        	//TODO: PUT this in virtual partition????
        	cf.setup(null, type.getIndexTableName(), false);
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
		MetaIdField idField = metaClass.getIdField();
		byte[] rowKey = idField.convertIdToNonVirtKey(pk);
		byte[] virtKey = idField.formVirtRowKey(rowKey);
		DboTableMeta metaDbo = metaClass.getMetaDbo();
		
		if(!metaClass.hasIndexedField(entity)) {
			session.remove(metaDbo, virtKey);
			return;
		} else if(!(entity instanceof NoSqlProxy)) {
			//then we don't have the database information for indexes so we need to read from the database
			proxy = find(metaClass.getMetaClass(), pk);
		}
		
		List<IndexData> indexToRemove = metaClass.findIndexRemoves((NoSqlProxy)proxy, rowKey);
		
		//REMOVE EVERYTHING HERE, we are probably removing extra and could optimize this later
		for(IndexData ind : indexToRemove) {
			session.removeFromIndex(metaDbo, ind.getColumnFamilyName(), ind.getRowKeyBytes(), ind.getIndexColumn());
		}
		
		session.remove(metaDbo, virtKey);
	}

	@Override
	public NoSqlTypedSession getTypedSession() {
		if(!isTypedSessionInitialized) {
			typedSession.setInformation(session, this);
		}
		return typedSession;
	}

	@Override
	public void clear() {
		session.clear();
	}

}
