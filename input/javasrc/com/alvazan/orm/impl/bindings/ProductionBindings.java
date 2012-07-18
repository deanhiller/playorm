package com.alvazan.orm.impl.bindings;

import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.spi.db.NoSqlRawSession;
import com.alvazan.orm.api.spi.index.IndexReaderWriter;
import com.alvazan.orm.api.spi.layer2.DboDatabaseMeta;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;
import com.alvazan.orm.layer2.nosql.cache.NoSqlReadCacheImpl;
import com.alvazan.orm.layer2.nosql.cache.NoSqlWriteCacheImpl;
import com.alvazan.orm.layer3.spi.db.cassandra.CassandraSession;
import com.alvazan.orm.layer3.spi.db.inmemory.InMemorySession;
import com.alvazan.orm.layer3.spi.index.inmemory.MemoryIndexWriter;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;

public class ProductionBindings implements Module {

	private DbTypeEnum type;
	private DboDatabaseMeta metaDb;

	public ProductionBindings(DbTypeEnum type) {
		this.type = type;
	}
	public ProductionBindings(DbTypeEnum type, DboDatabaseMeta metaDb) {
		this.type = type;
		this.metaDb = metaDb;
	}

	/**
	 * Mostly empty because we bind with annotations when we can.  Only third party bindings will
	 * end up in this file because we can't annotate third party objects
	 */
	@Override
	public void configure(Binder binder) {
		switch (type) {
		case CASSANDRA:
			binder.bind(NoSqlRawSession.class).to(CassandraSession.class);
			break;
		case IN_MEMORY:
			binder.bind(NoSqlRawSession.class).to(InMemorySession.class);
			binder.bind(IndexReaderWriter.class).to(MemoryIndexWriter.class);
			break;
		default:
			throw new RuntimeException("bug, unsupported database type="+type);
		}
		
		if(metaDb != null) //for adhoc query tool bind the meta database(which typically came from the database read)
			binder.bind(DboDatabaseMeta.class).toInstance(metaDb);
		else
			binder.bind(DboDatabaseMeta.class).asEagerSingleton();
		
		binder.bind(NoSqlSession.class).annotatedWith(Names.named("writecachelayer")).to(NoSqlWriteCacheImpl.class);
		binder.bind(NoSqlSession.class).annotatedWith(Names.named("readcachelayer")).to(NoSqlReadCacheImpl.class);
	}

}
