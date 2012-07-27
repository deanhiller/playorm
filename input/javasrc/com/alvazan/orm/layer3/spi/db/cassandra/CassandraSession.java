package com.alvazan.orm.layer3.spi.db.cassandra;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.spi2.DboDatabaseMeta;
import com.alvazan.orm.api.spi2.DboTableMeta;
import com.alvazan.orm.api.spi3.db.Action;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.NoSqlRawSession;
import com.alvazan.orm.api.spi3.db.Persist;
import com.alvazan.orm.api.spi3.db.Remove;
import com.alvazan.orm.api.spi3.db.Row;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.AstyanaxContext.Builder;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
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
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;

@SuppressWarnings({"unchecked", "rawtypes"})
public class CassandraSession implements NoSqlRawSession {

	private static final Logger log = LoggerFactory.getLogger(CassandraSession.class);
	private Keyspace keyspace;
	
	private Set<String> existingColumnFamilies = new HashSet<String>();
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
			existingColumnFamilies.add(def.getName());
		}
		log.info("Existing column families="+existingColumnFamilies+"\nNOTE: WE WILL CREATE " +
				"new column families automatically as you save entites that have no column family");
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
	
	public List<Row> findImpl(String colFamily, List<byte[]> keys) throws ConnectionException {
		if(!existingColumnFamilies.contains(colFamily)) {
			//well, if column family doesn't exist, then no entities exist either
			log.info("query was run on column family that does not yet exist="+colFamily);
			return createEmptyList(keys);
		}
		
		ColumnFamily<byte[], byte[]> cf = lookupOrCreate(colFamily, null);
		ColumnFamilyQuery<byte[], byte[]> query = keyspace.prepareQuery(cf);
		RowSliceQuery<byte[], byte[]> slice = query.getKeySlice(keys);
		OperationResult<Rows<byte[], byte[]>> result = slice.execute();
		Rows<byte[], byte[]> rows = result.getResult();
		
		List<Row> retVal = new ArrayList<Row>();
		for(byte[] key : keys) {
			com.netflix.astyanax.model.Row<byte[], byte[]> row = rows.getRow(key);
			if(row.getColumns().isEmpty()) {
				//Astyanax returns a row when there is none BUT we know if there are 0 columns there is really no row in the database
				//then
				retVal.add(null);
			} else {
				Row r = new Row();
				r.setKey(key);
				processColumns(row, r);
				retVal.add(r);
			}
		}
		
		return retVal;
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
			ColumnFamily cf = lookupOrCreate(action.getColFamily(), mgr);
			
			if(action instanceof Persist) {
				persist((Persist)action, cf, m);
			} else if(action instanceof Remove) {
				remove((Remove)action, cf, m);
			}
		}
		
		m.execute();
	}

	private ColumnFamily lookupOrCreate(String colFamily, NoSqlEntityManager mgr) throws ConnectionException {
		if(!existingColumnFamilies.contains(colFamily)) {
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
			
			Class columnNameType = cf.getColumnNameType();
			
			ColumnFamilyDefinition def = cluster.makeColumnFamilyDefinition()
				    .setName(colFamily)
				    .setKeyspace(keyspace.getKeyspaceName());
			
			if(String.class.equals(columnNameType)) 
				def = def.setComparatorType("UTF8Type");
			else if(Integer.class.equals(columnNameType)
					|| Long.class.equals(columnNameType)
					|| Short.class.equals(columnNameType)
					|| Byte.class.equals(columnNameType))
				def = def.setComparatorType("IntegerType");
			else if(Float.class.equals(columnNameType)
					|| Double.class.equals(columnNameType))
				def = def.setComparatorType("DecimalType");
			else
				throw new UnsupportedOperationException("Not supported yet, we need a BigDecimal comparator type here for sure");
			
			cluster.addColumnFamily(def);
			existingColumnFamilies.add(colFamily);
		}
		
		//should we cache this and just look it up each time or is KISS fine for now....
		ColumnFamily cf = new ColumnFamily(colFamily, BytesArraySerializer.get(), BytesArraySerializer.get());
		return cf;
	}

	private void remove(Remove action, ColumnFamily cf, MutationBatch m) {
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

	private void persist(Persist action, ColumnFamily cf, MutationBatch m) {
		ColumnListMutation colMutation = m.withRow(cf, action.getRowKey());
		
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
		for(String cf : existingColumnFamilies) {
			ColumnFamily colFamily = lookupOrCreate(cf, null);
			keyspace.truncateColumnFamily(colFamily); 
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
	
	public Iterable<Column> columnSliceImpl(String colFamily, byte[] rowKey,
			byte[] from, byte[] to, int batchSize) throws ConnectionException {
		if(!existingColumnFamilies.contains(colFamily)) {
			//well, if column family doesn't exist, then no entities exist either
			log.info("query was run on column family that does not yet exist="+colFamily);
			return new ArrayList<Column>();
		}
		
		ColumnFamily cf = lookupOrCreate(colFamily, null);
		
		ByteBufferRange build = new RangeBuilder().setStart(from).setEnd(to).setLimit(batchSize).build();
		
		return new OurIter(cf, rowKey, build);
	}
	
	private class OurIter implements Iterable<Column> {

		private ColumnFamily cf;
		private byte[] rowKey;
		private ByteBufferRange builder;

		public OurIter(ColumnFamily cf, byte[] rowKey, ByteBufferRange build) {
			this.cf = cf;
			this.rowKey = rowKey;
			this.builder = build;
		}

		@Override
		public Iterator<Column> iterator() {
			return new OurIterator(cf, rowKey, builder);
		}
	}
	private class OurIterator implements Iterator<Column> {
		private RowQuery<byte[], byte[]> query;
		private Iterator<com.netflix.astyanax.model.Column<byte[]>> subIterator;
		
		public OurIterator(ColumnFamily cf, byte[] rowKey,
				ByteBufferRange builder) {
			query = keyspace
					.prepareQuery(cf)
					.getKey(rowKey)
					.autoPaginate(true)
					.withColumnRange(builder);
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

			com.netflix.astyanax.model.Column<byte[]> col = subIterator.next();

			Column c = new Column();
			c.setName(col.getName());
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
