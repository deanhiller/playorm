package com.alvazan.orm.impl.bindings;

import java.util.Map;

import com.alvazan.orm.api.base.AbstractBootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.spi2.DboDatabaseMeta;
import com.alvazan.orm.api.spi2.NoSqlSessionFactory;
import com.alvazan.orm.api.spi3.db.NoSqlRawSession;
import com.alvazan.orm.api.spi3.db.conv.Converter;
import com.alvazan.orm.layer1.base.BaseEntityManagerFactoryImpl;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class Bootstrap extends AbstractBootstrap {

	@SuppressWarnings("rawtypes")
	@Override
	public NoSqlEntityManagerFactory createInstance(DbTypeEnum type, Map<String, String> properties, Map<Class, Converter> converters) {
		Injector injector = Guice.createInjector(new ProductionBindings(type));
		NoSqlEntityManagerFactory factory = injector.getInstance(NoSqlEntityManagerFactory.class);

		NoSqlRawSession inst = injector.getInstance(NoSqlRawSession.class);
		inst.start(properties);
		
		BaseEntityManagerFactoryImpl impl = (BaseEntityManagerFactoryImpl)factory;
		impl.setInjector(injector);
		
		//The expensive scan all entities occurs here...
		impl.setup(properties, converters);
		
		return impl;
	}

	/**
	 * A raw interface for non-ORM situations where flush can still be used to send all addIndex,removeIndex and persists, removes
	 * all at the same time.  This is especially useful as if addIndex fails BEFORE you flush, nothing is sent(no persists, removes)
	 * so there is nothing to really resolve.  We try to fail fast in persist, remove, addIndex, and removeIndex.  Then at flush
	 * time, we actually write out everything (at which point stuff can fail leading to inconsistency that needs to be cleaned up,
	 * but we will log everything that was being written on failure and what was success and what failed)
	 * 
	 * @param type
	 * @param metaDb 
	 * @return
	 */
	public static NoSqlSessionFactory createRawInstance(DbTypeEnum type, DboDatabaseMeta metaDb) {
		Injector injector = Guice.createInjector(new ProductionBindings(type, metaDb));
		return injector.getInstance(NoSqlSessionFactory.class);
	}
}
