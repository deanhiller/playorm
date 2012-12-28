package com.alvazan.orm.impl.bindings;

import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.NoSqlRawSession;
import com.alvazan.orm.api.z8spi.meta.DboDatabaseMeta;
import com.alvazan.orm.layer0.base.BaseEntityManagerFactoryImpl;
import com.alvazan.orm.layer5.nosql.cache.NoSqlReadCacheImpl;
import com.alvazan.orm.layer5.nosql.cache.NoSqlWriteCacheImpl;
import com.alvazan.orm.layer9z.spi.db.cassandra.CassandraSession;
import com.alvazan.orm.layer9z.spi.db.inmemory.InMemorySession;
import com.alvazan.orm.logging.NoSqlDevLogger;
import com.alvazan.orm.logging.NoSqlRawLogger;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;

public class ProductionBindings implements Module {

	private DbTypeEnum type;
	private NoSqlRawSession rawSession;

	public ProductionBindings(DbTypeEnum type, NoSqlRawSession temp) {
		this.type = type;
		this.rawSession = temp;
	}

	/**
	 * Mostly empty because we bind with annotations when we can.  Only third party bindings will
	 * end up in this file because we can't annotate third party objects
	 */
	@Override
	public void configure(Binder binder) {
		if(rawSession != null) {
			binder.bind(NoSqlRawSession.class).annotatedWith(Names.named("main")).toInstance(rawSession);
			bindRawSession("sub", binder);
		} else {
			bindRawSession("main", binder);
		}

		binder.bind(NoSqlEntityManagerFactory.class).to(BaseEntityManagerFactoryImpl.class);
		binder.bind(DboDatabaseMeta.class).asEagerSingleton();
		
		binder.bind(NoSqlRawSession.class).annotatedWith(Names.named("logger")).to(NoSqlRawLogger.class).asEagerSingleton();
		binder.bind(NoSqlSession.class).annotatedWith(Names.named("writecachelayer")).to(NoSqlWriteCacheImpl.class);
		binder.bind(NoSqlSession.class).annotatedWith(Names.named("readcachelayer")).to(NoSqlReadCacheImpl.class);
		binder.bind(NoSqlSession.class).annotatedWith(Names.named("logger")).to(NoSqlDevLogger.class);
	}

	private void bindRawSession(String name, Binder binder) {
		switch (type) {
		case CASSANDRA:
			binder.bind(NoSqlRawSession.class).annotatedWith(Names.named(name)).to(CassandraSession.class).asEagerSingleton();
			break;
		case IN_MEMORY:
			binder.bind(NoSqlRawSession.class).annotatedWith(Names.named(name)).to(InMemorySession.class).asEagerSingleton();
			break;
		default:
			throw new RuntimeException("bug, unsupported database type="+type);
		}		
	}

}
