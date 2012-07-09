package com.alvazan.orm.impl.bindings;

import com.alvazan.orm.api.base.AbstractBootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.spi.layer2.NoSqlSessionFactory;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class Bootstrap extends AbstractBootstrap {

	@Override
	public NoSqlEntityManagerFactory createInstance(DbTypeEnum type) {
		Injector injector = Guice.createInjector(new ProductionBindings(type));
		NoSqlEntityManagerFactory factory = injector.getInstance(NoSqlEntityManagerFactory.class);
		return factory;
	}

	/**
	 * A raw interface for non-ORM situations where flush can still be used to send all addIndex,removeIndex and persists, removes
	 * all at the same time.  This is especially useful as if addIndex fails BEFORE you flush, nothing is sent(no persists, removes)
	 * so there is nothing to really resolve.  We try to fail fast in persist, remove, addIndex, and removeIndex.  Then at flush
	 * time, we actually write out everything (at which point stuff can fail leading to inconsistency that needs to be cleaned up,
	 * but we will log everything that was being written on failure and what was success and what failed)
	 * 
	 * @param type
	 * @return
	 */
	public static NoSqlSessionFactory createRawInstance(DbTypeEnum type) {
		Injector injector = Guice.createInjector(new ProductionBindings(type));
		return injector.getInstance(NoSqlSessionFactory.class);
	}
}
