package com.alvazan.orm.impl.bindings;

import java.util.Map;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.z8spi.NoSqlRawSession;
import com.alvazan.orm.api.z8spi.conv.Converter;
import com.alvazan.orm.layer0.base.BaseEntityManagerFactoryImpl;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

public class BootstrapImpl extends Bootstrap {

	@SuppressWarnings("rawtypes")
	@Override
	public NoSqlEntityManagerFactory createInstance(DbTypeEnum type, Map<String, Object> properties, Map<Class, Converter> converters, ClassLoader cl2) {
		Injector injector = Guice.createInjector(new ProductionBindings(type));
		NoSqlEntityManagerFactory factory = injector.getInstance(NoSqlEntityManagerFactory.class);

		Named named = Names.named("logger");
		Key<NoSqlRawSession> key = Key.get(NoSqlRawSession.class, named);
		NoSqlRawSession inst = injector.getInstance(key);
		inst.start(properties);
		
		//why not just add setInjector() and setup() in NoSqlEntityManagerFactory
		BaseEntityManagerFactoryImpl impl = (BaseEntityManagerFactoryImpl)factory;
		impl.setInjector(injector);
		
		ClassLoader cl = cl2;
		if(cl == null)
			cl = BootstrapImpl.class.getClassLoader();
		//The expensive scan all entities occurs here...
		impl.setup(properties, converters, cl);
		
		return impl;
	}

}
