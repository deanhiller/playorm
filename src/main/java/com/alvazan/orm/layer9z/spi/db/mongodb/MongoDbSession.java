package com.alvazan.orm.layer9z.spi.db.mongodb;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	
	private Map<String, Info> collNameToMongodb = new HashMap<String, Info>();
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
		try {
			mongoClient = new MongoClient();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String keySpace = properties.get(Bootstrap.MONGODB_KEYSPACE).toString();
		db =  mongoClient.getDB(keySpace);
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
		DBCollection table = lookupColFamily(indexCfName, ormSession);
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
			//Only edit the row
				doc.put("k", keyToPersist);
		} else {
			doc = new BasicDBObject();
			// insert a new row
			doc.append("i", StandardConverters.convertFromBytes(String.class, rowKey));
			//doc.append("k",key);
			doc.put("k", keyToPersist);
			doc.append("v", value);
			table.insert(doc);
		}
	}

	private void removeIndex(RemoveIndex action, MetaLookup ormSession) {
		String colFamily = action.getIndexCfName();
		if (colFamily.equalsIgnoreCase("BytesIndice"))
			return;
		DBCollection table = lookupColFamily(colFamily, ormSession);
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

	private DBCollection lookupColFamily(String colFamily, MetaLookup mgr) {
		DBCollection table;
		if(db.collectionExists(colFamily)) {
			table = db.getCollection(colFamily);
			return table;
		}
		
		log.info("CREATING column family="+colFamily+" in the MongDB nosql store");
			
		DboTableMeta cf = dbMetaFromOrmOnly.getMeta(colFamily);
		if(cf == null) {
			//check the database now for the meta since it was not found in the ORM meta data.  This is for
			//those that are modifying meta data themselves
			//DboDatabaseMeta db = mgr.find(DboDatabaseMeta.class, DboDatabaseMeta.META_DB_ROWKEY);
			cf = mgr.find(DboTableMeta.class, colFamily);
			log.info("cf from db="+cf);
		}

		if(cf == null) {
			throw new IllegalStateException("Column family='"+colFamily+"' was not found AND we looked up meta data for this column" +
					" family to create it AND we could not find that data so we can't create it for you");
		}

		table = db.createCollection(colFamily, new BasicDBObject());
		if (colFamily.equalsIgnoreCase("StringIndice") || colFamily.equalsIgnoreCase("IntegerIndice") || colFamily.equalsIgnoreCase("DecimalIndice")) {
			BasicDBObject index = new BasicDBObject();
			index.append("i", 1);
			index.append("k", 1);
			table.ensureIndex(index);
		}
		Info info = new Info();
		info.setColumnType(null);
		info.setRowKeyType(null);
		info.setDbObj(table);
		collNameToMongodb.put(colFamily.toLowerCase(), info);
		return table;
	}

	private void remove(Remove action, MetaLookup ormSession) {
		String colFamily = action.getColFamily().getColumnFamily();
		DBCollection table = lookupColFamily(colFamily, ormSession);
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
			for (byte[] name : action.getColumns()) {
				row.removeField(StandardConverters.convertFromBytes(String.class, name));			}
		}
	}

	private void removeColumn(RemoveColumn action, MetaLookup ormSession) {

		String colFamily = action.getColFamily().getColumnFamily();
		DBCollection table = lookupColFamily(colFamily, ormSession);
		DBObject row = table.findOne(action.getRowKey());
		if (row != null)
			row.removeField(StandardConverters.convertFromBytes(String.class, action.getColumn()));
		//table.remove(row);
	}

	private void persist(Persist action, MetaLookup ormSession) {
		String colFamily = action.getColFamily().getColumnFamily();
		DBCollection table = lookupColFamily(colFamily, ormSession);
		BasicDBObject row = findOrCreateRow(table, action.getRowKey());
		BasicDBObject doc = new BasicDBObject();
		for(Column col : action.getColumns()) {
			byte[] value = new byte[0];
			if(col.getValue() != null)
				value = col.getValue();
			//doc.append(StandardConverters.convertFromBytes(String.class, col.getName()), value);
			doc.append(StandardConverters.convertToString(col.getName()), value);
		}
		table.findAndModify(row, doc);
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
		Set<String> collList = db.getCollectionNames();
		for (String coll : collList) {
			Info info = createInfo(coll, null, null);
			String lowerCaseName = coll.toLowerCase();
			collNameToMongodb.put(lowerCaseName, info);
		}
	}

	public Info createInfo(String dbCollection, ColumnType type, StorageTypeEnum keyType) {
		// Do we need to implement the types??
		Info info = new Info();
		info.setColumnType(type);
		info.setRowKeyType(keyType);
		info.setDbObj(db.getCollection(dbCollection));
		return info;
	}

	private Info fetchDbCollectionInfo(String tableName, MetaLookup mgr) {
		Info info = collNameToMongodb.get(tableName.toLowerCase());
		return info;
	}
}
