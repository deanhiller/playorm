package com.alvazan.orm.impl.bindings;

import com.alvazan.orm.api.NoSqlEntityManager;
import com.alvazan.orm.api.NoSqlEntityManagerFactory;
import com.alvazan.orm.impl.base.BaseEntityManagerFactoryImpl;
import com.alvazan.orm.impl.base.BaseEntityManagerImpl;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;

public class ProductionBindings implements Module {

	/**
	 * Mostly empty because we bind with annotations when we can.  Only third party bindings will
	 * end up in this file because we can't annotate third party objects
	 */
	@Override
	public void configure(Binder binder) {
		binder.bind(NoSqlEntityManagerFactory.class)
        	.annotatedWith(Names.named("baseFactory"))
        	.to(BaseEntityManagerFactoryImpl.class);
		binder.bind(NoSqlEntityManager.class)
			.annotatedWith(Names.named("baseManager"))
			.to(BaseEntityManagerImpl.class);
	}

}
