package com.alvazan.test;

import org.junit.Test;

import com.alvazan.orm.api.spi.db.NoSqlRawSession;
import com.alvazan.orm.api.spi.index.IndexReaderWriter;
import com.alvazan.orm.api.spi.index.SpiQueryAdapter;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;
import com.alvazan.orm.impl.meta.data.MetaQuery;
import com.alvazan.orm.impl.meta.query.MetaClassDbo;
import com.alvazan.orm.impl.meta.query.MetaFieldDbo;
import com.alvazan.orm.impl.meta.query.MetaInfoMap;
import com.alvazan.orm.impl.meta.scan.ScannerForQuery;
import com.alvazan.orm.layer2.nosql.cache.NoSqlReadCacheImpl;
import com.alvazan.orm.layer2.nosql.cache.NoSqlWriteCacheImpl;
import com.alvazan.orm.layer3.spi.db.inmemory.InMemorySession;
import com.alvazan.orm.layer3.spi.index.inmemory.MemoryIndexWriter;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;

public class TestLuceneQuery implements Module {

	@SuppressWarnings("rawtypes")
	@Test
	public void testBasic() {
		Injector injector = Guice.createInjector(this);
		ScannerForQuery scanner = injector.getInstance(ScannerForQuery.class);
		String sql = "select * FROM MyEntity e WHERE e.cat >= :hello and (e.mouse < :to and e.dog <:hithere) and e.cat>=:to";
		
		MetaQuery metaQuery = scanner.parseQuery(sql);
		
		SpiQueryAdapter spiMetaQuery = metaQuery.createSpiMetaQuery("indexName");
		spiMetaQuery.setParameter("to", "jianghuai");
		//spiMetaQuery.getResultList();
	}

	@Override
	public void configure(Binder binder) {
		MetaInfoMap map = new MetaInfoMap();
		addMetaClassDbo(map, "MyEntity", "cat", "mouse", "dog");
		addMetaClassDbo(map, "OtherEntity", "id", "dean", "declan", "pet", "house");
		
		binder.bind(MetaInfoMap.class).toInstance(map);
		binder.bind(IndexReaderWriter.class).to(MemoryIndexWriter.class).asEagerSingleton();
		binder.bind(NoSqlRawSession.class).to(InMemorySession.class);
	
		binder.bind(NoSqlSession.class).annotatedWith(Names.named("writecachelayer")).to(NoSqlWriteCacheImpl.class);
		binder.bind(NoSqlSession.class).annotatedWith(Names.named("readcachelayer")).to(NoSqlReadCacheImpl.class);		
	}

	private void addMetaClassDbo(MetaInfoMap map, String entityName, String ... fields) {
		MetaClassDbo meta = new MetaClassDbo();
		meta.setTableName(entityName);
		map.addMetaClassDbo(meta);
		
		for(String field : fields) {
			MetaFieldDbo fieldDbo = new MetaFieldDbo();
			fieldDbo.setName(field);
			meta.addField(fieldDbo);
		}
	}
}
