package com.alvazan.orm.api.base;

import java.util.Map;

import com.alvazan.orm.api.z8spi.SpiConstants;
import com.alvazan.orm.api.z8spi.conv.Converter;

@SuppressWarnings("rawtypes")
public abstract class Bootstrap {

	public static final String TYPE = "nosql.nosqltype";
	public static final String AUTO_CREATE_KEY = "nosql.autoCreateKey";
	public static final String LIST_OF_EXTRA_CLASSES_TO_SCAN_KEY = "nosql.listOfClassesToScan";
	
	public static final String CASSANDRA_BUILDER = SpiConstants.CASSANDRA_BUILDER;
	public static final String CASSANDRA_CLUSTERNAME = "nosql.cassandra.clusterName";
	public static final String CASSANDRA_KEYSPACE = "nosql.cassandra.keyspace";
	public static final String CASSANDRA_SEEDS = "nosql.cassandra.seeds";
	public static final String CASSANDRA_CF_CREATE_CALLBACK = SpiConstants.CASSANDRA_CF_CREATE_CALLBACK;
	
	public static final String MONGODB_CLUSTERNAME = "nosql.mongodb.clusterName";
	public static final String MONGODB_KEYSPACE = "nosql.mongodb.keyspace";
	public static final String MONGODB_SEEDS = "nosql.mongodb.seeds";

	public static final String HBASE_CLUSTERNAME = "nosql.hbase.clusterName";
	public static final String HBASE_KEYSPACE = "nosql.hbase.keyspace";
	public static final String HBASE_SEEDS = "nosql.hbase.seeds";

	private static final String OUR_IMPL = "com.alvazan.orm.impl.bindings.BootstrapImpl";
	public static final String SPI_IMPL = "nosql.spi.implementation";
	
	public synchronized static NoSqlEntityManagerFactory create(Map<String, Object> properties) {
		return create(properties, Bootstrap.class.getClassLoader());
	}
	
	public synchronized static NoSqlEntityManagerFactory create(Map<String, Object> properties, ClassLoader cl) {
		String type = (String) properties.get(TYPE);
		DbTypeEnum dbType;
		if("inmemory".equals(type)) {
			dbType = DbTypeEnum.IN_MEMORY;
		} else if("cassandra".equals(type)) {
			dbType = DbTypeEnum.CASSANDRA;
			
			String clusterName = (String) properties.get(CASSANDRA_CLUSTERNAME);
			String keyspace = (String) properties.get(CASSANDRA_KEYSPACE);
			String seeds = (String) properties.get(CASSANDRA_SEEDS);
			if(clusterName == null || keyspace == null || seeds == null)
				throw new IllegalArgumentException("Must supply the nosql.cassandra.* properties.  Read Bootstrap.java for values");
			createAndAddBestCassandraConfiguration(properties, clusterName, keyspace, seeds);
			
		} else
			throw new IllegalArgumentException("NoSql type="+type+" not supported. Read Bootstrap.java for possible values");
		
		return create(dbType, properties, null, cl);		
	}
	
	public synchronized static NoSqlEntityManagerFactory create(DbTypeEnum type, Map<String, Object> properties) {
		return create(type, properties, null, Bootstrap.class.getClassLoader());
	}
	public synchronized static NoSqlEntityManagerFactory create(DbTypeEnum type, Map<String, Object> properties, Map<Class, Converter> converters, ClassLoader cl) {
		return create(type, OUR_IMPL, properties, converters, cl);
	}
	
	public synchronized static NoSqlEntityManagerFactory create(DbTypeEnum type, String impl, Map<String, Object> properties, Map<Class, Converter> converters, ClassLoader cl) {
		Bootstrap newInstance = createInstance(impl);
		NoSqlEntityManagerFactory inst = newInstance.createInstance(type, properties, converters, cl);
		return inst;
	}

	private static Bootstrap createInstance(String impl) {
		try {
			Class<?> clazz = Class.forName(impl);
			Bootstrap newInstance = (Bootstrap) clazz.newInstance();
			return newInstance;
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	protected abstract NoSqlEntityManagerFactory createInstance(DbTypeEnum type, Map<String, Object> properties, Map<Class, Converter> converters, ClassLoader cl);

	public static void createAndAddBestCassandraConfiguration(
			Map<String, Object> properties, String clusterName,
			String keyspace, String seeds) {
		Bootstrap bootstrap = createInstance(OUR_IMPL);
		bootstrap.createBestCassandraConfig(properties, clusterName, keyspace, seeds);
	}

	protected abstract void createBestCassandraConfig(Map<String, Object> properties,
			String clusterName, String keyspace2, String seeds2);

	public static void createAndAddBestMongoDbConfiguration(Map<String, Object> properties, String clusterName, String keyspace, String seeds) {
		Bootstrap bootstrap = createInstance(OUR_IMPL);
		bootstrap.createBestMongoDbConfig(properties, clusterName, keyspace, seeds);
	}

	protected abstract void createBestMongoDbConfig(Map<String, Object> properties,
			String clusterName, String keyspace2, String seeds2);

	public static void createAndAddBestHBaseConfiguration(Map<String, Object> properties, String clusterName, String keyspace, String seeds) {
		Bootstrap bootstrap = createInstance(OUR_IMPL);
		bootstrap.createBestHBaseConfig(properties, clusterName, keyspace, seeds);
	}

	protected abstract void createBestHBaseConfig(Map<String, Object> properties,
			String clusterName, String keyspace2, String seeds2);

}
