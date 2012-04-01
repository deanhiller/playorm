package com.alvazan.orm.api;

import com.alvazan.orm.impl.bindings.InMemoryBinding;
import com.alvazan.orm.impl.bindings.ProductionBindings;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class Bootstrap {
	private static Injector injector;
	private static NoSqlEntityManagerFactory factory;
	
	public synchronized static NoSqlEntityManagerFactory getSingleton() {
		if(factory == null) {
			injector = Guice.createInjector(new ProductionBindings());
			factory = injector.getInstance(NoSqlEntityManagerFactory.class);
		}
		return factory;
	}
	
	public synchronized static NoSqlEntityManagerFactory getSingletonWithInMemoryNoSqlDb() {
		if(factory == null) {
			injector = Guice.createInjector(new ProductionBindings(), new InMemoryBinding());
			factory = injector.getInstance(NoSqlEntityManagerFactory.class);
		}
		return factory;		
	}
}
