package com.alvazan.orm.api.base;

import java.util.Map;

import com.alvazan.orm.api.spi3.meta.conv.Converter;
import com.alvazan.orm.api.spi9.db.SpiConstants;

@SuppressWarnings("rawtypes")
public abstract class Bootstrap {

	public static final String AUTO_CREATE_KEY = "nosql.autoCreateKey";
	public static final String LIST_OF_EXTRA_CLASSES_TO_SCAN_KEY = "nosql.listOfClassesToScan";
	public static final String SEEDS = "nosql.host";
	public static final String KEYSPACE = "nosql.keyspace";
	public static final String CLUSTER_NAME = "nosql.clustername";
	public static final String CASSANDRA_BUILDER = SpiConstants.CASSANDRA_BUILDER;

	public synchronized static NoSqlEntityManagerFactory create(DbTypeEnum type, Map<String, Object> properties, Map<Class, Converter> converters, ClassLoader cl) {
		return create(type, "com.alvazan.orm.impl.bindings.BootstrapImpl", properties, converters, cl);
	}
	
	public synchronized static NoSqlEntityManagerFactory create(DbTypeEnum type, String impl, Map<String, Object> properties, Map<Class, Converter> converters, ClassLoader cl) {
		try {
			Class<?> clazz = Class.forName(impl);
			Bootstrap newInstance = (Bootstrap) clazz.newInstance();
			NoSqlEntityManagerFactory inst = newInstance.createInstance(type, properties, converters, cl);
			
			return inst;
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	protected abstract NoSqlEntityManagerFactory createInstance(DbTypeEnum type, Map<String, Object> properties, Map<Class, Converter> converters, ClassLoader cl);

}
