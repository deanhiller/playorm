package com.alvazan.orm.api;

import com.alvazan.orm.impl.bindings.InMemoryBinding;
import com.alvazan.orm.impl.bindings.ProductionBindings;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class Bootstrap {

	public synchronized static NoSqlEntityManagerFactory create() {
		Injector injector = Guice.createInjector(new ProductionBindings());
		NoSqlEntityManagerFactory factory = injector.getInstance(NoSqlEntityManagerFactory.class);
		return factory;
	}
	
	public synchronized static NoSqlEntityManagerFactory createWithInMemoryDb() {
		Injector injector = Guice.createInjector(new ProductionBindings(), new InMemoryBinding());
		NoSqlEntityManagerFactory factory = injector.getInstance(NoSqlEntityManagerFactory.class);
		return factory;		
	}
}
