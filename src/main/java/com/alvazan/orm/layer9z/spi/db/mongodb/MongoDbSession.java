package com.alvazan.orm.layer9z.spi.db.mongodb;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Cache;
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
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
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
		try {
			mongoClient = new MongoClient();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String keySpace = properties.get(Bootstrap.MONGODB_KEYSPACE).toString();
		db =  mongoClient.getDB(keySpace);
	}

	@Override
	public void close() {
		mongoClient.close();
	}

	@Override
	public void readMetaAndCreateTable(MetaLookup ormSession, String colFamily) {
		
	}

	@Override
	public AbstractCursor<Column> columnSlice(DboTableMeta colFamily, byte[] rowKey,
			byte[] from, byte[] to, Integer batchSize, BatchListener l,
			MetaLookup mgr) {
		return null;
	}

	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scan, Key from,
			Key to, Integer batchSize, BatchListener l, MetaLookup mgr) {
		byte[] rowKey = scan.getRowKey();
		String indexTableName = scan.getIndexColFamily();
		CursorOfIndexes cursor = new CursorOfIndexes(rowKey, batchSize, l, rowProvider, indexTableName, from, to);
		cursor.setupMore(db);
		return cursor;
	}

	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scanInfo,
			List<byte[]> values, BatchListener list, MetaLookup mgr) {
		return null;
	}

	@Override
	public AbstractCursor<KeyValue<Row>> find(DboTableMeta colFamily,
			DirectCursor<byte[]> rowKeys, Cache cache, int batchSize,
			BatchListener list, MetaLookup mgr) {
		Info info = fetchColumnFamilyInfo(colFamily.getColumnFamily(), mgr);
		if(info == null) {
			//If there is no column family in mongodb, then we need to return no rows to the user...
			return new CursorReturnsEmptyRows2(rowKeys);
		}
		
/*		ColumnType type = info.getColumnType();
		if(type != ColumnType.ANY_EXCEPT_COMPOSITE) {
			throw new UnsupportedOperationException("Finding on composite type="+colFamily+" not allowed here, you should be using column slice as these rows are HUGE!!!!");
		}*/
		
		CursorKeysToRowsMDB cursor = new CursorKeysToRowsMDB(rowKeys, batchSize, list, rowProvider, colFamily);
		cursor.setupMore(db, colFamily, info, cache);
		return cursor;
	}

	private void persistIndex(PersistIndex action, MetaLookup ormSession) {
		String indexCfName = action.getIndexCfName();
		DBCollection table = lookupColFamily(indexCfName, ormSession);
		byte[] rowKey = action.getRowKey();
		IndexColumn column = action.getColumn();
		//BasicDBObject row = findOrCreateRow(table, rowKey);
		BasicDBObject doc = new BasicDBObject();
		doc.append("i", StandardConverters.convertFromBytes(String.class, rowKey));
		byte[] key = column.getIndexedValue();
		byte[] value = column.getPrimaryKey();
		if (key != null) {
			// Currently not inserting null values and also inserting the values in different ways
			doc.append("k",key);
			// table.findAndModify(row, doc);
			// The below is the best way to insert a new column
			//row.update(row, new BasicDBObject("$set", doc));
		}
		doc.append("v", value);
		table.insert(doc);
	}

	private void removeIndex(RemoveIndex action, MetaLookup ormSession) {
		String colFamily = action.getIndexCfName();
		if (colFamily.equalsIgnoreCase("BytesIndice"))
			return;
		DBCollection table = lookupColFamily(colFamily, ormSession);
		byte[] rowKey = action.getRowKey();
		IndexColumn column = action.getColumn();
	
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

		/*SortType sortType;
		StorageTypeEnum prefixType = cf.getColNamePrefixType();
		if(prefixType == null) {
			switch (cf.getNameStorageType()) {
			case BYTES:
				sortType = SortType.BYTES;
				break;
			case DECIMAL:
				sortType = SortType.DECIMAL;
				break;
			case INTEGER:
				sortType = SortType.INTEGER;
				break;
			case STRING:
				sortType = SortType.UTF8;
				break;
			default:
				throw new UnsupportedOperationException("type not supported="+cf.getNameStorageType());
			}
		} else {
			switch(prefixType) {
			case DECIMAL:
				sortType = SortType.DECIMAL_PREFIX;
				break;
			case INTEGER:
				sortType = SortType.INTEGER_PREFIX;
				break;
			case STRING:
				sortType = SortType.UTF8_PREFIX;
				break;
			default:
				throw new UnsupportedOperationException("type not supported="+prefixType);
			}
		}*/
		table = db.createCollection(colFamily, new BasicDBObject());
		if (colFamily.equalsIgnoreCase("StringIndice") || colFamily.equalsIgnoreCase("IntegerIndice") || colFamily.equalsIgnoreCase("DecimalIndice")) {
			BasicDBObject index = new BasicDBObject();
			index.append("i", 1);
			index.append("k", 1);
			table.ensureIndex(index);
		}
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
		for (byte[] name : action.getColumns()) {
			DBObject row = table.findOne(name);
			table.remove(row);
		}
	}

	private void removeColumn(RemoveColumn action, MetaLookup ormSession) {

		String colFamily = action.getColFamily().getColumnFamily();
		DBCollection table = lookupColFamily(colFamily, ormSession);
		DBObject row = table.findOne(action.getColumn());
		table.remove(row);
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
			doc.append(StandardConverters.convertFromBytes(String.class, col.getName()), value);
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

	private Info fetchColumnFamilyInfo(String string, MetaLookup mgr) {
		//return null;
		return new Info();
	}
}
