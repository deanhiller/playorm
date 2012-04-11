package com.alvazan.orm.impl.bindings;

import com.alvazan.orm.api.NoSqlEntityManager;
import com.alvazan.orm.api.NoSqlEntityManagerFactory;
import com.alvazan.orm.layer1.base.BaseEntityManagerFactoryImpl;
import com.alvazan.orm.layer1.base.BaseEntityManagerImpl;
import com.alvazan.orm.layer2.nosql.NoSqlSession;
import com.alvazan.orm.layer2.nosql.cache.NoSqlReadCacheImpl;
import com.alvazan.orm.layer2.nosql.cache.NoSqlWriteCacheImpl;
import com.alvazan.orm.layer3.spi.NoSqlRawSession;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;

public class ProductionBindings implements Module {

	/**
	 * Mostly empty because we bind with annotations when we can.  Only third party bindings will
	 * end up in this file because we can't annotate third party objects
	 */
	@Override
	public void configure(Binder binder) {
		binder.bind(NoSqlEntityManagerFactory.class)
        	.annotatedWith(Names.named("baseFactory"))
        	.to(BaseEntityManagerFactoryImpl.class);
		binder.bind(NoSqlEntityManager.class)
			.annotatedWith(Names.named("baseManager"))
			.to(BaseEntityManagerImpl.class);
		
		binder.bind(NoSqlSession.class).annotatedWith(Names.named("writecachelayer")).to(NoSqlWriteCacheImpl.class);
		binder.bind(NoSqlSession.class).annotatedWith(Names.named("readcachelayer")).to(NoSqlReadCacheImpl.class);
	}

}
