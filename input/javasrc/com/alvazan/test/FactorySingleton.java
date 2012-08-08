package com.alvazan.test;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.AbstractBootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;

public class FactorySingleton {

	private static final Logger log = LoggerFactory.getLogger(FactorySingleton.class);
	
	private static NoSqlEntityManagerFactory factory;
	
	public synchronized static NoSqlEntityManagerFactory createFactoryOnce() {
		if(factory == null) {
			log.info("CREATING FACTORY FOR TESTS");
			Map<String, Object> props = new HashMap<String, Object>();
			props.put(NoSqlEntityManagerFactory.AUTO_CREATE_KEY, "create");
			factory = AbstractBootstrap.create(DbTypeEnum.IN_MEMORY, props, null, null);
		}
		return factory;
	}
}
