package com.alvazan.orm.api;

import com.alvazan.orm.impl.bindings.ProductionBindings;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class Bootstrap {

	public synchronized static NoSqlEntityManagerFactory create(DbTypeEnum type) {
		Injector injector = Guice.createInjector(new ProductionBindings(type));
		NoSqlEntityManagerFactory factory = injector.getInstance(NoSqlEntityManagerFactory.class);
		return factory;
	}

}
