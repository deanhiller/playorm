package com.alvazan.orm.layer9z.spi.db.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.inject.Inject;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Cache;
import com.alvazan.orm.api.z8spi.ColumnType;
import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.MetaLookup;
import com.alvazan.orm.api.z8spi.NoSqlRawSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.Action;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.action.Persist;
import com.alvazan.orm.api.z8spi.action.PersistIndex;
import com.alvazan.orm.api.z8spi.action.Remove;
import com.alvazan.orm.api.z8spi.action.RemoveColumn;
import com.alvazan.orm.api.z8spi.action.RemoveIndex;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboDatabaseMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.mongodb.ServerAddress;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoDbSession implements NoSqlRawSession {

	private static final Logger log = LoggerFactory.getLogger(MongoDbSession.class);
	
	private DB db;
	private MongoClient mongoClient;
	
	@Inject
	private Provider<Row> rowProvider;

	@Inject
	private DboDatabaseMeta dbMetaFromOrmOnly;
	
	private Map<String, Info> cfNameToMongodb = new HashMap<String, Info>();
	private Map<String, String> virtualToCfName = new HashMap<String, String>();

	public DB getDb() {
		return db;
	}

	public void setDb(DB db) {
		this.db = db;
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	public void setMongoClient(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}
	
	@Override
	public void sendChanges(List<Action> actions, MetaLookup ormSession) {
		sendChangesImpl(actions, ormSession);
	}
	
	public void sendChangesImpl(List<Action> actions, MetaLookup ormSession) {
		for(Action action : actions) {
			if(action instanceof Persist) {
				persist((Persist)action, ormSession);
			} else if(action instanceof Remove) {
				remove((Remove)action, ormSession);
			} else if(action instanceof PersistIndex) {
				persistIndex((PersistIndex) action, ormSession);
			} else if(action instanceof RemoveIndex) {
				removeIndex((RemoveIndex) action, ormSession);
			} else if(action instanceof RemoveColumn) {
				removeColumn((RemoveColumn) action, ormSession);
			}
		}
	}
	

	@Override
	public void clearDatabase() {
		mongoClient.dropDatabase(db.getName());
	}

	@Override
	public void start(Map<String, Object> properties) {
		properties.keySet();
		String seeds = properties.get(Bootstrap.MONGODB_SEEDS).toString();
		try {
			if (seeds == null)
				mongoClient = new MongoClient();
			else if (seeds.contains(",")) {
				// It has multiple nodes
				StringTokenizer st = new StringTokenizer(seeds, ",");
				List<ServerAddress> addrs = new ArrayList<ServerAddress>();
				while (st.hasMoreElements()) {
					String serverName = st.nextElement().toString();
					ServerAddress server = new ServerAddress(serverName);
					addrs.add(server);
				}
				mongoClient = new MongoClient(addrs);
			} else
				mongoClient = new MongoClient(seeds);
		} catch (UnknownHostException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		String keySpace = properties.get(Bootstrap.MONGODB_KEYSPACE).toString();
		db = mongoClient.getDB(keySpace);
		findExistingCollections();
	}

	@Override
	public void close() {
		mongoClient.close();
	}

	@Override
	public void readMetaAndCreateTable(MetaLookup ormSession, String colFamily) {
		
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public AbstractCursor<Column> columnSlice(DboTableMeta colFamily, byte[] rowKey,
			byte[] from, byte[] to, Integer batchSize, BatchListener l,
			MetaLookup mgr) {
		Info info = fetchDbCollectionInfo(colFamily.getColumnFamily(), mgr);
		if(info == null) {
			//If there is no column family in mongodb, then we need to return no rows to the user...
			return null;
		}
		CursorColumnSliceMDB cursor = new CursorColumnSliceMDB(colFamily, l, batchSize, db, rowKey, from, to);
		return cursor;
	}

	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scan, Key from,
			Key to, Integer batchSize, BatchListener l, MetaLookup mgr) {
		byte[] rowKey = scan.getRowKey();
		String indexTableName = scan.getIndexColFamily();
		DboColumnMeta colMeta = scan.getColumnName();
		CursorOfIndexes cursor = new CursorOfIndexes(rowKey, batchSize, l, indexTableName, from, to);
		cursor.setupMore(db, colMeta);
		return cursor;
	}

	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scanInfo,
			List<byte[]> values, BatchListener list, MetaLookup mgr) {
		byte[] rowKey = scanInfo.getRowKey();
		String indexTableName = scanInfo.getIndexColFamily();
		DboColumnMeta colMeta = scanInfo.getColumnName();
		CursorForValues cursor = new CursorForValues(rowKey, list, indexTableName, values);
		cursor.setupMore(db, colMeta);
		return cursor;
	}

	@Override
	public AbstractCursor<KeyValue<Row>> find(DboTableMeta colFamily,
			DirectCursor<byte[]> rowKeys, Cache cache, int batchSize,
			BatchListener list, MetaLookup mgr) {
		Info info = fetchDbCollectionInfo(colFamily.getColumnFamily(), mgr);
		if(info == null) {
			//If there is no column family in mongodb, then we need to return no rows to the user...
			return new CursorReturnsEmptyRows2(rowKeys);
		}
		CursorKeysToRowsMDB cursor = new CursorKeysToRowsMDB(rowKeys, batchSize, list, rowProvider, colFamily);
		cursor.setupMore(db, colFamily, info, cache);
		return cursor;
	}

	private void persistIndex(PersistIndex action, MetaLookup ormSession) {
		String indexCfName = action.getIndexCfName();
		Info info = lookupOrCreate2(indexCfName, ormSession);
		DBCollection table = info.getDbObj();
		byte[] rowKey = action.getRowKey();
		IndexColumn column = action.getColumn();
		byte[] key = column.getIndexedValue();
		byte[] value = column.getPrimaryKey();
		BasicDBObject doc = findIndexRow(table, rowKey, value);
		Object keyToPersist = null;
		if (indexCfName.equalsIgnoreCase("StringIndice")) {
			keyToPersist = StandardConverters.convertFromBytes(String.class, key);
		} else if ((indexCfName.equalsIgnoreCase("IntegerIndice"))) {
			keyToPersist = StandardConverters.convertFromBytes(Integer.class, key);
		} else if (indexCfName.equalsIgnoreCase("DecimalIndice")) {
			keyToPersist = StandardConverters.convertFromBytes(Double.class, key);
		}
		if (doc != null) {
			// Check for duplicates
			Object indValue = doc.get("k");
			if (indValue != null && !indValue.equals(keyToPersist))
				insertIndex(table, rowKey, keyToPersist, value);
		} else
			insertIndex(table, rowKey, keyToPersist, value);
	}

	private void insertIndex(DBCollection table, byte[] rowKey,
			Object keyToPersist, byte[] value) {
		BasicDBObject doc = new BasicDBObject();
		doc.append("i",	StandardConverters.convertFromBytes(String.class, rowKey));
		doc.put("k", keyToPersist);
		doc.append("v", value);
		table.insert(doc);
	}

	private void removeIndex(RemoveIndex action, MetaLookup ormSession) {
		String colFamily = action.getIndexCfName();
		if (colFamily.equalsIgnoreCase("BytesIndice"))
			return;
		Info info = fetchDbCollectionInfo(colFamily, ormSession);
		DBCollection table = info.getDbObj();
		byte[] rowKey = action.getRowKey();
		IndexColumn column = action.getColumn();
		byte[] value = column.getPrimaryKey();
		BasicDBObject doc = findIndexRow(table, rowKey, value);
		if (doc == null) {
			log.info("Index: " + column.toString() + " already removed.");
		} else {
			table.remove(doc);
		}
	}

	private synchronized Exception createColFamily(String virtualCf, MetaLookup ormSession) {
		try {
			long start = System.currentTimeMillis();
			createColFamilyImpl(virtualCf, ormSession);
			long total = System.currentTimeMillis() - start;
			if(log.isInfoEnabled())
				log.info("Total time to CREATE column family in MongoDb and wait for all nodes to update="+total);
		} catch(Exception e) {
			log.trace("maybe someone else created at same time, so hold off on throwing exception", e);
			return e;
		}
		return null;
	}

	private synchronized void createColFamilyImpl(String virtualCf, MetaLookup ormSession) {
		if(lookupVirtCf(virtualCf) != null)
			return;

		log.info("CREATING column family="+virtualCf+" in MongoDb database= "+db);
		
		DboTableMeta meta = loadFromInMemoryOrDb(virtualCf, ormSession);
		log.info("CREATING REAL cf="+meta.getRealColumnFamily()+" (virtual CF="+meta.getRealVirtual()+")");

		createColFamilyInMongoDb(meta);
	}

	private void createColFamilyInMongoDb(DboTableMeta meta) {
		String colFamily = meta.getRealColumnFamily();
		// final check before creating
		if (db!=null && db.collectionExists(colFamily))
			return;
		DBCollection table = db.createCollection(colFamily, new BasicDBObject());
		if (colFamily.equalsIgnoreCase("StringIndice")
				|| colFamily.equalsIgnoreCase("IntegerIndice")
				|| colFamily.equalsIgnoreCase("DecimalIndice")) {
			BasicDBObject index = new BasicDBObject();
			index.append("i", 1);
			index.append("k", 1);
			table.ensureIndex(index);
		}
		String virtual = meta.getColumnFamily();
		String realCf = meta.getRealColumnFamily();
		String realCfLower = realCf.toLowerCase();
		Info info = createInfo(realCf, null, null);
		virtualToCfName.put(virtual, realCfLower);
		cfNameToMongodb.put(realCfLower, info);
	}

	private void remove(Remove action, MetaLookup ormSession) {
		String colFamily = action.getColFamily().getColumnFamily();
		Info info = fetchDbCollectionInfo(colFamily, ormSession);
		DBCollection table = info.getDbObj();
		if(action.getAction() == null)
			throw new IllegalArgumentException("action param is missing ActionEnum so we know to remove entire row or just columns in the row");
		switch(action.getAction()) {
		case REMOVE_ENTIRE_ROW:
			DBObject row = table.findOne(action.getRowKey());
			table.remove(row);
			break;
		case REMOVE_COLUMNS_FROM_ROW:
			removeColumns(action, table);
			break;
		default:
			throw new RuntimeException("bug, unknown remove action="+action.getAction());
		}
	}

	private void removeColumns(Remove action, DBCollection table) {
		DBObject row = table.findOne(action.getRowKey());
		if (row != null) {
			BasicDBObject docToRemove = new BasicDBObject();
			for (byte[] name : action.getColumns()) {
				docToRemove.append(StandardConverters.convertToString(name), 1);
			}
			table.update(row, new BasicDBObject("$unset", docToRemove));
		}
	}

	private void removeColumn(RemoveColumn action, MetaLookup ormSession) {

		String colFamily = action.getColFamily().getColumnFamily();
		Info info = fetchDbCollectionInfo(colFamily, ormSession);
		DBCollection table = info.getDbObj();
		DBObject row = table.findOne(action.getRowKey());
		BasicDBObject docToRemove = new BasicDBObject();
		docToRemove.put(StandardConverters.convertToString(action.getColumn()), 1);
		if (row != null)
			table.update(row, new BasicDBObject("$unset", docToRemove));
	}

	private void persist(Persist action, MetaLookup ormSession) {
		String colFamily = action.getColFamily().getColumnFamily();
		Info info = lookupOrCreate2(colFamily, ormSession);
		DBCollection table = info.getDbObj();
		BasicDBObject row = findOrCreateRow(table, action.getRowKey());
		BasicDBObject doc = new BasicDBObject();
		for(Column col : action.getColumns()) {
			byte[] value = new byte[0];
			if(col.getValue() != null)
				value = col.getValue();
			doc.append(StandardConverters.convertToString(col.getName()), value);
		}
		table.update(row, new BasicDBObject("$set", doc));
	}

	public BasicDBObject findOrCreateRow(DBCollection table, byte[] key) {
		DBObject row = table.findOne(key);
		if(row == null) {
			BasicDBObject basicRow = new BasicDBObject();
			basicRow.append("_id", key);
			table.insert(basicRow);
			return basicRow;
		}
		else return (BasicDBObject)row;
	}

	public BasicDBObject findIndexRow(DBCollection table, byte[] indexKey, byte[] key) {
		BasicDBObject basicRow = new BasicDBObject();
		basicRow.append("i", StandardConverters.convertFromBytes(String.class, indexKey));
		basicRow.append("v", key);
		DBObject row = table.findOne(basicRow);
		if(row == null) {
			return null;
		}
		else return (BasicDBObject)row;
	}

	private void findExistingCollections() {
		Set<String> collectionList = db.getCollectionNames();
		for (String coll : collectionList) {
			addExistingCollections(coll);
		}
	}

	private void addExistingCollections(String collectionName) {
			Info info = createInfo(collectionName, null, null);
			String lowerCaseName = collectionName.toLowerCase();
			cfNameToMongodb.put(lowerCaseName, info);
	}

	private Info createInfo(String dbCollection, ColumnType type, StorageTypeEnum keyType) {
		// Do we need to implement the types??
		Info info = new Info();
		info.setColumnType(type);
		info.setRowKeyType(keyType);
		info.setDbObj(db.getCollection(dbCollection));
		return info;
	}

	private Info fetchDbCollectionInfo(String tableName, MetaLookup mgr) {
		Info info = lookupVirtCf(tableName);
		if(info != null)
			return info;
		
		//in rare circumstances, there may be a new column family that was created by another server we need to load into
		//memory for ourselves
		return tryToLoadColumnFamilyVirt(tableName, mgr);
	}

	private Info lookupVirtCf(String virtualCf) {
		String cfName = virtualToCfName.get(virtualCf);
		if (log.isDebugEnabled())
			log.debug("looking up virtualcf=" + virtualCf + " actual name="
					+ cfName);
		if (cfName == null)
			return null;

		Info info = cfNameToMongodb.get(cfName);
		if (log.isDebugEnabled())
			log.debug("virtual=" + virtualCf + " actual name=" + cfName
					+ " cf info=" + info);
		return info;
	}

	private DboTableMeta loadFromInMemoryOrDb(String virtCf, MetaLookup lookup) {
		log.info("looking up meta=" + virtCf
				+ " so we can add table to memory(one time operation)");
		DboTableMeta meta = dbMetaFromOrmOnly.getMeta(virtCf);
		if (meta != null) {
			log.info("found meta=" + virtCf + " locally");
			return meta;
		}

		DboTableMeta table = lookup.find(DboTableMeta.class, virtCf);
		if (table == null)
			throw new IllegalArgumentException(
					"We can't load the meta for virtual or real CF="
							+ virtCf
							+ " because there is not meta found in DboTableMeta table");
		log.info("found meta=" + virtCf + " in database");
		return table;
	}

	private void loadColumnFamily(DBCollection def, String virtCf, String realCf) {
		addExistingCollections(def.getName());
		virtualToCfName.put(virtCf, realCf);
	}

	public Info lookupOrCreate2(String virtualCf, MetaLookup ormSession) {
		// There is a few possibilities here
		// 1. Another server already created the CF while we were online in
		// which case we just need to load it into memory
		// 2. No one has created the CF yet

		// fetch will load from MongoDb if we don't have it in-memory
		Info origInfo = fetchDbCollectionInfo(virtualCf, ormSession);
		Exception ee = null;
		if (origInfo == null) {
			// no one has created the CF yet so we need to create it.
			ee = createColFamily(virtualCf, ormSession);
		}

		// Now check...maybe someone else created it...or we did
		// successfully....
		Info info = fetchDbCollectionInfo(virtualCf, ormSession);
		if (info == null)
			throw new RuntimeException(
					"Could not create and could not find virtual or real colfamily="
							+ virtualCf
							+ " see chained exception AND it could be your name is not allowed as a valid MongoDb Column Family name",
					ee);
		return info;
	}


	private Info tryToLoadColumnFamilyVirt(String virtColFamily,
			MetaLookup lookup) {
		long start = System.currentTimeMillis();
		Info info = tryToLoadColumnFamilyImpl(virtColFamily, lookup);
		long total = System.currentTimeMillis() - start;
		if (log.isInfoEnabled())
			log.info("Total time to LOAD column family meta from MongoDb="
					+ total);
		return info;
	}

	private Info tryToLoadColumnFamilyImpl(String virtCf, MetaLookup lookup) {
		synchronized (virtCf.intern()) {
			log.info("Column family NOT found in-memory=" + virtCf + ", CHECK and LOAD from MongoDb if available");

			String cfName = virtualToCfName.get(virtCf);
			if (cfName != null) {
				Info info = cfNameToMongodb.get(cfName);
				if (info != null) {
					log.info("NEVER MIND, someone beat us to loading it into memory, it is now there=" + virtCf + "(realcf=" + cfName + ")");
					return cfNameToMongodb.get(cfName);
				}
			}

			DboTableMeta table = loadFromInMemoryOrDb(virtCf, lookup);

			String realCf = table.getRealColumnFamily();

			String realCfLower = realCf.toLowerCase();
			Info info = cfNameToMongodb.get(realCfLower);
			if (info != null) {
				log.info("Virt CF=" + virtCf + " already exists and real colfamily=" + realCf + " already exists so return it");
				// Looks like it already existed
				String cfLowercase = realCf.toLowerCase();
				virtualToCfName.put(virtCf, cfLowercase);
				return info;
			}

			// NOW, the schema appears stable, let's get that column family and load it
			if (db != null && db.collectionExists(realCf)) {
				log.info("coooool, we found a new column family=" + realCf + "(virt=" + virtCf 	+ ") to load so we are going to load that for you so every future operation is FAST");
				DBCollection dbCollection = db.getCollection(realCf);

				loadColumnFamily(dbCollection, virtCf, realCf);
				return lookupVirtCf(virtCf);
			} else {
				log.info("Well, we did NOT find any column family=" + realCf
						+ " to load in MongoDb(from virt=" + virtCf + ")");
				return null;
			}
		}
	}

}
