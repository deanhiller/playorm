package com.alvazan.orm.layer9.spi.db.cassandra;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.DboDatabaseMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.api.spi3.meta.StorageTypeEnum;
import com.alvazan.orm.api.spi9.db.ColumnType;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.AstyanaxContext.Builder;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class ColumnFamilyHelper {
	private static final Logger log = LoggerFactory.getLogger(ColumnFamilyHelper.class);
	
	@Inject
	private DboDatabaseMeta dbMetaFromOrmOnly;
	
	private Map<String, Info> existingColumnFamilies2 = new HashMap<String, Info>();

	private Keyspace keyspace;
	private Cluster cluster;
	private String keyspaceName;
	
	public Keyspace getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(Keyspace keyspace) {
		this.keyspace = keyspace;
	}

	public Cluster getCluster() {
		return cluster;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	public String getKeyspaceName() {
		return keyspaceName;
	}

	public void setKeyspaceName(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	public void start(Map<String, Object> properties) throws ConnectionException {
		Object seedsObj = properties.get(Bootstrap.SEEDS);
		Object keyspaceNameObj = properties.get(Bootstrap.KEYSPACE);
		Object clusterNameObj = properties.get(Bootstrap.CLUSTER_NAME);
		if(seedsObj == null || !(seedsObj instanceof String))
			throw new IllegalArgumentException("The property Bootstrap.HOST was not in the Map or was in the Map but not as a String");
		else if(keyspaceNameObj == null || !(keyspaceNameObj instanceof String))
			throw new IllegalArgumentException("The property Bootstrap.KEYSPACE was not in the Map or was in the Map but not as a String");
		else if(clusterNameObj == null || !(clusterNameObj instanceof String))
			throw new IllegalArgumentException("The property Bootstrap.CLUSTER_NAME was not in the Map or was in the Map but not as a String");
		
		String clusterName = (String) clusterNameObj; //"SDICluster";
		keyspaceName = (String) keyspaceNameObj; // "SDIKeyspace";
		String seeds = (String) seedsObj;
		Builder builder = new AstyanaxContext.Builder()
	    .forCluster(clusterName)
	    .forKeyspace(keyspaceName)
	    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
	        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
	    )
	    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
	        .setMaxConnsPerHost(2)
	        .setInitConnsPerHost(2)
	        .setSeeds(seeds)
	    )
	    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor());
		
		AstyanaxContext<Cluster> clusterContext = builder.buildCluster(ThriftFamilyFactory.getInstance());
		clusterContext.start();
		
		cluster = clusterContext.getEntity();
		List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();
		boolean exists = false;
		for(KeyspaceDefinition kDef : keyspaces) {
			if(keyspaceName.equalsIgnoreCase(kDef.getName())) {
				exists = true;
				break;
			}
		}

		if(!exists) {
			KeyspaceDefinition def = cluster.makeKeyspaceDefinition();
			def.setName(keyspaceName);
			def.setStrategyClass("org.apache.cassandra.locator.SimpleStrategy");
			Map<String, String> map = new HashMap<String, String>();
			map.put("replication_factor", "3");
			def.setStrategyOptions(map);
			cluster.addKeyspace(def);
		}
		
		AstyanaxContext<Keyspace> context = builder.buildKeyspace(ThriftFamilyFactory.getInstance());
		context.start();
		
		keyspace = context.getEntity();

		KeyspaceDefinition keySpaceMeta = keyspace.describeKeyspace();
		
		findExistingColumnFamilies(keySpaceMeta);
		log.info("Existing column families="+existingColumnFamilies2+"\nNOTE: WE WILL CREATE " +
				"new column families automatically as you save entites that have no column family");
	}

	private void findExistingColumnFamilies(KeyspaceDefinition keySpaceMeta) {
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
				cf = new ColumnFamily(colFamily, BytesArraySerializer.get(), BytesArraySerializer.get());
				break;
			case COMPOSITE_DECIMALPREFIX:
			case COMPOSITE_INTEGERPREFIX:
			case COMPOSITE_STRINGPREFIX:
				com.netflix.astyanax.serializers.
				AnnotatedCompositeSerializer<GenericComposite> eventSerializer = new AnnotatedCompositeSerializer<GenericComposite>(GenericComposite.class);
				cf = new ColumnFamily<byte[], GenericComposite>(colFamily, BytesArraySerializer.get(), eventSerializer);
				info.setCompositeSerializer(eventSerializer);
				break;
			default:
				throw new UnsupportedOperationException("type not supported yet="+type);
		}
		
		info.setColumnFamilyObj(cf);
		return info;
	}
	
	public Info lookupOrCreate2(String colFamily, NoSqlEntityManager mgr) {
		String cf = colFamily.toLowerCase();
		if(existingColumnFamilies2.get(cf) == null) {
			createColFamily(colFamily, mgr);
		}
		
		return fetchColumnFamilyInfo(colFamily);
	}

	public Info fetchColumnFamilyInfo(String colFamily) {
		String cf = colFamily.toLowerCase();
		Info info = existingColumnFamilies2.get(cf);
		return info;
	}
	
	private synchronized void createColFamily(String colFamily, NoSqlEntityManager mgr) {
		if(existingColumnFamilies2.get(colFamily.toLowerCase()) != null)
			return;
			
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
	
	private ColumnFamilyDefinition setColumnNameCompareType(DboTableMeta cf,
			StorageTypeEnum type, ColumnFamilyDefinition def2) {
		ColumnFamilyDefinition def = def2;
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
			ColumnFamilyDefinition def2) {
		ColumnFamilyDefinition def = def2;
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
			break;
		case BYTES:
			break;
		default:
			throw new UnsupportedOperationException("type="+rowKeyType+" is not supported at this time");
		}
		return def;
	}

	private void addColumnFamily(ColumnFamilyDefinition def) {
		try {
			cluster.addColumnFamily(def);
		} catch (ConnectionException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
}
