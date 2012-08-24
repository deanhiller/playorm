package com.alvazan.orm.impl.bindings;

import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.spi3.meta.DboDatabaseMeta;
import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi9.db.NoSqlRawSession;
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

	public ProductionBindings(DbTypeEnum type) {
		this.type = type;
	}

	/**
	 * Mostly empty because we bind with annotations when we can.  Only third party bindings will
	 * end up in this file because we can't annotate third party objects
	 */
	@Override
	public void configure(Binder binder) {
		switch (type) {
		case CASSANDRA:
			binder.bind(NoSqlRawSession.class).annotatedWith(Names.named("main")).to(CassandraSession.class).asEagerSingleton();
			break;
		case IN_MEMORY:
			binder.bind(NoSqlRawSession.class).annotatedWith(Names.named("main")).to(InMemorySession.class).asEagerSingleton();
			break;
		default:
			throw new RuntimeException("bug, unsupported database type="+type);
		}
		
		binder.bind(DboDatabaseMeta.class).asEagerSingleton();
		
		binder.bind(NoSqlRawSession.class).annotatedWith(Names.named("logger")).to(NoSqlRawLogger.class).asEagerSingleton();
		binder.bind(NoSqlSession.class).annotatedWith(Names.named("writecachelayer")).to(NoSqlWriteCacheImpl.class);
		binder.bind(NoSqlSession.class).annotatedWith(Names.named("readcachelayer")).to(NoSqlReadCacheImpl.class);
		binder.bind(NoSqlSession.class).annotatedWith(Names.named("logger")).to(NoSqlDevLogger.class);
	}

}
