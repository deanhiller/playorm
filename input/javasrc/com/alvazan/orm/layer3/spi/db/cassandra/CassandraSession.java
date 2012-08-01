package com.alvazan.orm.layer3.spi.db.cassandra;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.inject.Inject;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.spi2.DboColumnMeta;
import com.alvazan.orm.api.spi2.DboDatabaseMeta;
import com.alvazan.orm.api.spi2.DboTableMeta;
import com.alvazan.orm.api.spi2.StorageTypeEnum;
import com.alvazan.orm.api.spi3.db.Action;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.ColumnType;
import com.alvazan.orm.api.spi3.db.IndexColumn;
import com.alvazan.orm.api.spi3.db.NoSqlRawSession;
import com.alvazan.orm.api.spi3.db.Persist;
import com.alvazan.orm.api.spi3.db.PersistIndex;
import com.alvazan.orm.api.spi3.db.Remove;
import com.alvazan.orm.api.spi3.db.Row;
import com.alvazan.orm.api.spi3.db.conv.StandardConverters;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.AstyanaxContext.Builder;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.BigIntegerSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.CompositeRangeBuilder;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;

@SuppressWarnings({"unchecked", "rawtypes"})
public class CassandraSession implements NoSqlRawSession {

	private static final Logger log = LoggerFactory.getLogger(CassandraSession.class);
	private Keyspace keyspace;
	
	private Map<String, Info> existingColumnFamilies2 = new HashMap<String, Info>();
	private Cluster cluster;
	
	@Inject
	private DboDatabaseMeta dbMetaFromOrmOnly;
	
	@Override
	public void start(Map<String, String> properties) {
		try {
			startImpl(properties);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void startImpl(Map<String, String> properties) throws ConnectionException {
		Builder builder = new AstyanaxContext.Builder()
	    .forCluster("SDICluster")
	    .forKeyspace("SDIKeyspace")
	    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
	        .setDiscoveryType(NodeDiscoveryType.NONE)
	    )
	    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
	        .setPort(9160)
	        .setMaxConnsPerHost(1)
	        .setSeeds("127.0.0.1:9160")
	    )
	    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor());
		
		AstyanaxContext<Keyspace> context = builder.buildKeyspace(ThriftFamilyFactory.getInstance());
		context.start();
		
		AstyanaxContext<Cluster> clusterContext = builder.buildCluster(ThriftFamilyFactory.getInstance());
		clusterContext.start();
		
		keyspace = context.getEntity();
		cluster = clusterContext.getEntity();
		KeyspaceDefinition keySpaceMeta = keyspace.describeKeyspace();
		
		List<ColumnFamilyDefinition> cfList = keySpaceMeta.getColumnFamilyList();
		for(ColumnFamilyDefinition def : cfList) {
			String comparatorType = def.getComparatorType();
			ColumnType type = ColumnType.ANY_EXCEPT_COMPOSITE;
			if(formName(UTF8Type.class, BytesType.class).equals(comparatorType)) {
				type = ColumnType.COMPOSITE_STRINGPREFIX;
			} else if(formName(IntegerType.class, BytesType.class).equals(comparatorType)) {
				type = ColumnType.COMPOSITE_INTEGERPREFIX;
			} else if(formName(DecimalType.class, BytesType.class).equals(comparatorType)) {
				type = ColumnType.COMPOSITE_DECIMALPREFIX;
			}
			
			String keyValidationClass = def.getKeyValidationClass();
			StorageTypeEnum keyType = null;
			if(UTF8Type.class.getName().equals(keyValidationClass)) {
				keyType = StorageTypeEnum.STRING;
			} else if(DecimalType.class.getName().equals(keyValidationClass)) {
				keyType = StorageTypeEnum.DECIMAL;
			} else if(IntegerType.class.getName().equals(keyValidationClass)) {
				keyType = StorageTypeEnum.INTEGER;
			} else if(BytesType.class.getName().equals(keyValidationClass)) {
				keyType = StorageTypeEnum.BYTES;
			}
			
			String colFamily = def.getName();
			Info info = createInfo(colFamily, type, keyType);
			String lowerCaseName = colFamily.toLowerCase();
			existingColumnFamilies2.put(lowerCaseName, info);
		}
		log.info("Existing column families="+existingColumnFamilies2+"\nNOTE: WE WILL CREATE " +
				"new column families automatically as you save entites that have no column family");
	}

	private String formName(Class class1, Class class2) {
		return CompositeType.class.getName()+"("+class1.getName()+","+class2.getName()+")";
	}

	public Info createInfo(String colFamily, ColumnType type, StorageTypeEnum keyType) {
		if(keyType == null)
			return null;
		
		Info info = new Info();
		info.setColumnType(type);
		info.setRowKeyType(keyType);
		
		//should we cache this and just look it up each time or is KISS fine for now....
		ColumnFamily cf = null;
		switch(type) {
			case ANY_EXCEPT_COMPOSITE:
				Serializer s = BytesArraySerializer.get();
				if(info.getRowKeyType() == StorageTypeEnum.STRING)
					s = StringSerializer.get();
				else if(info.getRowKeyType() == StorageTypeEnum.INTEGER)
					s = BigIntegerSerializer.get();
				else if(info.getRowKeyType() == StorageTypeEnum.DECIMAL)
					s = BigDecimalSerializer.get();
				cf = new ColumnFamily(colFamily, s, BytesArraySerializer.get());
				break;
			case COMPOSITE_DECIMALPREFIX:
				com.netflix.astyanax.serializers.
				AnnotatedCompositeSerializer<BigDecimalComposite> eventSerializer = new AnnotatedCompositeSerializer<BigDecimalComposite>(BigDecimalComposite.class);
				cf = new ColumnFamily<String, BigDecimalComposite>(colFamily, StringSerializer.get(), eventSerializer);
				info.setCompositeSerializer(eventSerializer);
				break;
			case COMPOSITE_INTEGERPREFIX:
				AnnotatedCompositeSerializer<BigIntegerComposite> bigIntSer = new AnnotatedCompositeSerializer<BigIntegerComposite>(BigIntegerComposite.class);
				cf = new ColumnFamily<String, BigIntegerComposite>(colFamily, StringSerializer.get(), bigIntSer);
				info.setCompositeSerializer(bigIntSer);
				break;
			case COMPOSITE_STRINGPREFIX:
				AnnotatedCompositeSerializer<StringComposite> stringSerializer = new AnnotatedCompositeSerializer<StringComposite>(StringComposite.class);
				cf = new ColumnFamily<String, StringComposite>(colFamily, StringSerializer.get(), stringSerializer);
				info.setCompositeSerializer(stringSerializer);
				break;
			default:
				throw new UnsupportedOperationException("type not supported yet="+type);
		}
		
		info.setColumnFamilyObj(cf);
		return info;
	}

	@Override
	public void stop() {

	}
	
	@Override	
	public List<Row> find(String colFamily, List<byte[]> keys) {
		try {
			return findImpl(colFamily, keys);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	
	public List<Row> findImpl(String colFamily, List<byte[]> keys2) throws ConnectionException {
		Info info = fetchColumnFamilyInfo(colFamily);
		if(info == null) {
			//well, if column family doesn't exist, then no entities exist either
			log.info("query was run on column family that does not yet exist="+colFamily);
			return createEmptyList(keys2);
		}

		ColumnFamily cf = info.getColumnFamilyObj();
		ColumnType type = info.getColumnType();
		if(type != ColumnType.ANY_EXCEPT_COMPOSITE) {
			throw new UnsupportedOperationException("Finding on composite type not allowed here, you should be using column slice as these rows are HUGE!!!!");
		}
		
		List keys = createKeys(info.getRowKeyType(), keys2);
		
		ColumnFamilyQuery<byte[], byte[]> query = keyspace.prepareQuery(cf);
		RowSliceQuery<byte[], byte[]> slice = query.getKeySlice(keys);
		OperationResult<Rows<byte[], byte[]>> result = slice.execute();
		Rows rows = result.getResult();
		
		List<Row> retVal = new ArrayList<Row>();
		for(Object key : keys) {
			com.netflix.astyanax.model.Row<byte[], byte[]> row = rows.getRow(key);
			if(row.getColumns().isEmpty()) {
				//Astyanax returns a row when there is none BUT we know if there are 0 columns there is really no row in the database
				//then
				retVal.add(null);
			} else {
				Row r = new Row();
				byte[] keyData = StandardConverters.convertToBytes(key);
				r.setKey(keyData);
				processColumns(row, r);
				retVal.add(r);
			}
		}
		
		return retVal;
	}


	private List createKeys(StorageTypeEnum rowKeyType, List<byte[]> keys2) {
		List realKeys = new ArrayList();
		for(byte[] key : keys2) {
			Object k = StandardConverters.convertFromBytes(rowKeyType.getJavaType(), key);
			realKeys.add(k);
		}
		return realKeys;
	}

	@SuppressWarnings("unused")
	private List<Row> createEmptyList(List<byte[]> keys) {
		List<Row> rows = new ArrayList<Row>();
		for(byte[] key : keys) {
			rows.add(null);
		}
		return rows;
	}

	private void processColumns(
			com.netflix.astyanax.model.Row<byte[], byte[]> row, Row r) {
		for(com.netflix.astyanax.model.Column<byte[]> col : row.getColumns()) {
			byte[] name = col.getName();
			byte[] val = col.getByteArrayValue();
			long timestamp = col.getTimestamp();
			Column c = new Column();
			c.setName(name);
			if(val.length != 0)
				c.setValue(val);
			c.setTimestamp(timestamp);
			
			r.put(name, c);
		}
	}

	@Override
	public void sendChanges(List<Action> actions, Object ormSession) {
		try {
			sendChangesImpl(actions, (NoSqlEntityManager) ormSession);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	public void sendChangesImpl(List<Action> actions, NoSqlEntityManager mgr) throws ConnectionException {
		MutationBatch m = keyspace.prepareMutationBatch();
		for(Action action : actions) {
			if(action instanceof PersistIndex) {
				persistIndex((PersistIndex)action, mgr, m);
			} else if(action instanceof Persist) {
				persist((Persist)action, mgr, m);
			} else if(action instanceof Remove) {
				remove((Remove)action, m);
			}
		}
		
		m.execute();
	}

	private Info lookupOrCreate2(String colFamily, NoSqlEntityManager mgr) {
		String cf = colFamily.toLowerCase();
		if(existingColumnFamilies2.get(cf) == null) {
			createColFamily(colFamily, mgr);
		}
		
		return fetchColumnFamilyInfo(colFamily);
	}

	private Info fetchColumnFamilyInfo(String colFamily) {
		String cf = colFamily.toLowerCase();
		Info info = existingColumnFamilies2.get(cf);
		return info;
	}
	
	private void createColFamily(String colFamily, NoSqlEntityManager mgr)
			 {
		log.info("CREATING column family="+colFamily+" in cassandra");
		
		DboTableMeta cf = dbMetaFromOrmOnly.getMeta(colFamily);
		if(cf == null) {
			//check the database now for the meta since it was not found in the ORM meta data.  This is for
			//those that are modifying meta data themselves
			DboDatabaseMeta db = mgr.find(DboDatabaseMeta.class, DboDatabaseMeta.META_DB_ROWKEY);
			cf = db.getMeta(colFamily);
		}
		
		
		if(cf == null) {
			throw new IllegalStateException("Column family='"+colFamily+"' was not found AND we looked up meta data for this column" +
					" family to create it AND we could not find that data so we can't create it for you");
		}

		ColumnFamilyDefinition def = cluster.makeColumnFamilyDefinition()
			    .setName(colFamily)
			    .setKeyspace(keyspace.getKeyspaceName());

		StorageTypeEnum rowKeyType = cf.getIdColumnMeta().getStorageType();
		StorageTypeEnum type = cf.getColNamePrefixType();
		def = addRowKeyValidation(cf, def);
		def = setColumnNameCompareType(cf, type, def);
		
		ColumnType colType = ColumnType.ANY_EXCEPT_COMPOSITE;
		if(type == StorageTypeEnum.STRING)
			colType = ColumnType.COMPOSITE_STRINGPREFIX;
		else if(type == StorageTypeEnum.INTEGER)
			colType = ColumnType.COMPOSITE_INTEGERPREFIX;
		else if(type == StorageTypeEnum.DECIMAL)
			colType = ColumnType.COMPOSITE_DECIMALPREFIX;
		
		addColumnFamily(def);
		Info info = createInfo(colFamily, colType, rowKeyType);
		String cfName = colFamily.toLowerCase();
		existingColumnFamilies2.put(cfName, info);
	}

	private void addColumnFamily(ColumnFamilyDefinition def) {
		try {
			cluster.addColumnFamily(def);
		} catch (ConnectionException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	private ColumnFamilyDefinition setColumnNameCompareType(DboTableMeta cf,
			StorageTypeEnum type, ColumnFamilyDefinition def) {
		if(type == null) {
			StorageTypeEnum t = cf.getNameStorageType();
			if(t == StorageTypeEnum.STRING)
				def = def.setComparatorType("UTF8Type");
			else if(t == StorageTypeEnum.DECIMAL)
				def = def.setComparatorType("DecimalType");
			else if(t == StorageTypeEnum.INTEGER)
				def = def.setComparatorType("IntegerType");
			else
				throw new UnsupportedOperationException("type="+type+" is not supported at this time");
		} else if(type == StorageTypeEnum.STRING) {
			def = def.setComparatorType("CompositeType(UTF8Type, BytesType)");
		} else if(type == StorageTypeEnum.INTEGER) {
			def = def.setComparatorType("CompositeType(IntegerType, BytesType)");
		} else if(type == StorageTypeEnum.DECIMAL) {
			def = def.setComparatorType("CompositeType(DecimalType, BytesType)");
		}
		else
			throw new UnsupportedOperationException("Not supported yet, we need a BigDecimal comparator type here for sure");
		return def;
	}

	private ColumnFamilyDefinition addRowKeyValidation(DboTableMeta cf,
			ColumnFamilyDefinition def) {
		DboColumnMeta idColumnMeta = cf.getIdColumnMeta();
		StorageTypeEnum rowKeyType = idColumnMeta.getStorageType();
		switch (rowKeyType) {
		case STRING:
			def = def.setKeyValidationClass("UTF8Type");
			break;
		case INTEGER:
			def = def.setKeyValidationClass("DecimalType");
			break;
		case DECIMAL:
			def = def.setKeyValidationClass("IntegerType");
		case BYTES:
			break;
		default:
			throw new UnsupportedOperationException("type="+rowKeyType+" is not supported at this time");
		}
		return def;
	}

	private void remove(Remove action, MutationBatch m) {
		Info info = fetchColumnFamilyInfo(action.getColFamily());
		if(info == null)
			return; //if no cf exist/returned, nothing to do
		ColumnFamily cf = info.getColumnFamilyObj();
		
		switch(action.getAction()) {
		case REMOVE_ENTIRE_ROW:
			m.withRow(cf, action.getRowKey()).delete();
			break;
		case REMOVE_COLUMNS_FROM_ROW:
			removeColumns(action, cf, m);
			break;
		default:
			throw new RuntimeException("bug, unknown remove action="+action.getAction());
		}
	}

	private void removeColumns(Remove action, ColumnFamily cf, MutationBatch m) {
		ColumnListMutation row = m.withRow(cf, action.getRowKey());
		
		for(byte[] name : action.getColumns()) {
			row.deleteColumn(name);
		}
	}

	private void persistIndex(PersistIndex action, NoSqlEntityManager mgr, MutationBatch m) {
		Info info = lookupOrCreate2(action.getColFamily(), mgr);
		ColumnFamily cf = info.getColumnFamilyObj();
		
		String key = toUTF8(action.getRowKey());
		ColumnListMutation colMutation = m.withRow(cf, key);
		
		byte[] indexedValue = action.getColumn().getIndexedValue();
		byte[] pk = action.getColumn().getPrimaryKey();
		
		ColumnType type = action.getColumnType();
		Object toPersist;
		switch(type) {
		case COMPOSITE_STRINGPREFIX:
			StringComposite comp = new StringComposite();
			comp.value = toUTF8(indexedValue);
			comp.pk = pk;
			toPersist = comp;
			break;
		case COMPOSITE_INTEGERPREFIX:
			BigIntegerComposite bigInt = new BigIntegerComposite();
			bigInt.value = StandardConverters.convertFromBytes(BigInteger.class, indexedValue);
			bigInt.pk = pk;
			toPersist = bigInt;
			break;
		case COMPOSITE_DECIMALPREFIX:
			BigDecimalComposite bigDec = new BigDecimalComposite();
			bigDec.value = StandardConverters.convertFromBytes(BigDecimal.class, indexedValue);
			bigDec.pk = pk;
			toPersist = pk;
			break;
		default:
			throw new UnsupportedOperationException("not supported at this time. type="+type);
		}

		colMutation.putEmptyColumn(toPersist);
	}
	
	private void persist(Persist action, NoSqlEntityManager mgr, MutationBatch m) {
		
		Info info = lookupOrCreate2(action.getColFamily(), mgr);
		ColumnFamily cf = info.getColumnFamilyObj();
		
		String key = toUTF8(action.getRowKey());
		ColumnListMutation colMutation = m.withRow(cf, key);
		
		for(Column col : action.getColumns()) {
			Integer theTime = null;
			Long time = col.getTimestamp();
			if(time != null)
				theTime = (int)time.longValue();
			byte[] value = new byte[0];
			if(col.getValue() != null)
				value = col.getValue();
			
			colMutation.putColumn(col.getName(), value, theTime);
		}
	}

	@Override
	public void clearDatabaseIfInMemoryType() {
		try {
			clearImpl();
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	public void clearImpl() throws ConnectionException {
		for(String cf : existingColumnFamilies2.keySet()) {
			Info info = fetchColumnFamilyInfo(cf);
			if(info != null) {
				ColumnFamily colFamily = info.getColumnFamilyObj();
				keyspace.truncateColumnFamily(colFamily);
			}
		}
	}

	@Override
	public Iterable<Column> columnRangeScan(String colFamily, byte[] rowKey,
			byte[] from, byte[] to, int batchSize) {
		try {
			return columnSliceImpl(colFamily, rowKey, from, to, batchSize);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	
	private Iterable<Column> columnSliceImpl(String colFamily, byte[] rowKey,
			byte[] from, byte[] to, int batchSize) throws ConnectionException {
		Info info = fetchColumnFamilyInfo(colFamily);
		if(info == null) {
			//well, if column family doesn't exist, then no entities exist either
			log.info("query was run on column family that does not yet exist="+colFamily);
			return new ArrayList<Column>();
		}

		ColumnType type = info.getColumnType();
		ColumnFamily cf = info.getColumnFamilyObj();
		if(type == ColumnType.ANY_EXCEPT_COMPOSITE)
			return findBasic(rowKey, from, to, batchSize, info);
		else if(type == ColumnType.COMPOSITE_STRINGPREFIX)
			return findString(rowKey, from, to, batchSize, info);
		
		throw new UnsupportedOperationException("not done here yet");
	}

	private Iterable<Column> findString(byte[] rowKey, byte[] from, byte[] to,
			int batchSize, Info info) {
		ColumnFamily cf = info.getColumnFamilyObj();
		AnnotatedCompositeSerializer serializer = info.getCompositeSerializer();

		String val1 = toUTF8(from);
		String val2 = toUTF8(to);
		
		CompositeRangeBuilder range = serializer.buildRange().greaterThanEquals(val1).lessThanEquals(val2).limit(batchSize);

		ColumnFamilyQuery query = keyspace.prepareQuery(cf);

		Object key = info.getRowKeyType().convertFromNoSql(rowKey);
		RowQuery rowQuery = query.getKey(key)
							.withColumnRange(range)
							.autoPaginate(true);

		return new OurIter(cf, rowQuery, info);
	}

	private String toUTF8(byte[] rowKey) {
		try {
			return new String(rowKey, "UTF8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	
	
	private Iterable<Column> findBasic(byte[] rowKey, byte[] from, byte[] to,
			int batchSize, Info info) {
		ColumnFamily cf = info.getColumnFamilyObj();
		ByteBufferRange build = new RangeBuilder().setStart(from).setEnd(to).setLimit(batchSize).build();
		
		ColumnFamilyQuery query = keyspace.prepareQuery(cf);
		Object key = info.getRowKeyType().convertFromNoSql(rowKey);
		RowQuery rowQuery = query.getKey(key)
							.autoPaginate(true)
							.withColumnRange(build);
		
		return new OurIter(cf, rowQuery, info);
	}
	
	private class OurIter implements Iterable<Column> {

		private ColumnFamily cf;
		private RowQuery query;
		private Info info;

		public OurIter(ColumnFamily cf, RowQuery query2, Info info) {
			this.cf = cf;
			this.query = query2;
			this.info = info;
		}

		@Override
		public Iterator<Column> iterator() {
			return new OurIterator(cf, query, info);
		}
	}
	private class OurIterator implements Iterator<Column> {
		private RowQuery<byte[], byte[]> query;
		private Iterator<com.netflix.astyanax.model.Column<byte[]>> subIterator;
		private Info info;
		
		public OurIterator(ColumnFamily cf, RowQuery query, Info info) {
			this.query = query;
			this.info = info;
		}

		@Override
		public boolean hasNext() {
			fetchMoreResults();
			if(subIterator == null)
				return false;
			
			return subIterator.hasNext();
		}

		@Override
		public Column next() {
			fetchMoreResults();
			if(subIterator == null)
				throw new ArrayIndexOutOfBoundsException("no more elements");

			com.netflix.astyanax.model.Column col = subIterator.next();

			Object obj = col.getName();
			byte[] name;
			switch (info.getColumnType()) {
			case ANY_EXCEPT_COMPOSITE:
				name = (byte[])obj;
				break;
			case COMPOSITE_STRINGPREFIX:
				StringComposite str = (StringComposite)obj;
				name = str.pk;
				break;
			case COMPOSITE_INTEGERPREFIX:
				BigIntegerComposite bigInt = (BigIntegerComposite)obj;
				name = bigInt.pk;
				break;
			case COMPOSITE_DECIMALPREFIX:
				BigDecimalComposite bigDec = (BigDecimalComposite)obj;
				name = bigDec.pk;
				break;
			default:
				throw new UnsupportedOperationException("type not supported yet="+info.getColumnType());
			}
			
			Column c = new Column();
			c.setName(name);
			c.setValue(col.getByteArrayValue());
			c.setTimestamp(col.getTimestamp());
			
			return c;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported");
		}
		
		
		private void fetchMoreResults() {
			try {
				fetchMoreResultsImpl();
			} catch (ConnectionException e) {
				throw new RuntimeException(e);
			}
		}
		
		private void fetchMoreResultsImpl() throws ConnectionException {
			if(subIterator != null && subIterator.hasNext())
				return; //no need to fetch more since subIterator has more
			
			ColumnList<byte[]> columns = query.execute().getResult();
			if(columns.isEmpty())
				subIterator = null; 
			else 
				subIterator = columns.iterator();
		}
	}

}
