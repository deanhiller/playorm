package com.alvazan.test;

import com.alvazan.orm.api.base.AbstractBootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;

public class FactorySingleton {

	private static NoSqlEntityManagerFactory factory;
	
	public synchronized static NoSqlEntityManagerFactory createFactoryOnce() {
		if(factory == null) {
			factory = AbstractBootstrap.create(DbTypeEnum.IN_MEMORY);
			factory.setup(null, "com.alvazan.test.db");
		}
		return factory;
	}
}
