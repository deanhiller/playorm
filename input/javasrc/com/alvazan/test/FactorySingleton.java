package com.alvazan.test;

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
			factory = AbstractBootstrap.create(DbTypeEnum.IN_MEMORY);
			factory.setup(null, "com.alvazan.test.db");
		}
		return factory;
	}
}
