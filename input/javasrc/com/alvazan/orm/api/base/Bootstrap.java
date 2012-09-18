package com.alvazan.orm.api.base;

import java.util.Map;

import com.alvazan.orm.api.z8spi.SpiConstants;
import com.alvazan.orm.api.z8spi.conv.Converter;

@SuppressWarnings("rawtypes")
public abstract class Bootstrap {

	public static final String AUTO_CREATE_KEY = "nosql.autoCreateKey";
	public static final String LIST_OF_EXTRA_CLASSES_TO_SCAN_KEY = "nosql.listOfClassesToScan";
	public static final String CASSANDRA_BUILDER = SpiConstants.CASSANDRA_BUILDER;
	private static final String OUR_IMPL = "com.alvazan.orm.impl.bindings.BootstrapImpl";
	public static final String SPI_IMPL = "nosql.spi.implementation";
	
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
}
