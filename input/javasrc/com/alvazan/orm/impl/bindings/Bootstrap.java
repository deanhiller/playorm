package com.alvazan.orm.impl.bindings;

import com.alvazan.orm.api.AbstractBootstrap;
import com.alvazan.orm.api.DbTypeEnum;
import com.alvazan.orm.api.NoSqlEntityManagerFactory;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class Bootstrap extends AbstractBootstrap {

	@Override
	public NoSqlEntityManagerFactory createInstance(DbTypeEnum type) {
		Injector injector = Guice.createInjector(new ProductionBindings(type));
		NoSqlEntityManagerFactory factory = injector.getInstance(NoSqlEntityManagerFactory.class);
		return factory;
	}

}
