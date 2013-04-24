package org.playorm.cron.bindings;

import java.util.Map;

import org.playorm.cron.api.CronService;
import org.playorm.cron.api.CronServiceFactory;
import org.playorm.cron.impl.CronServiceImpl;

import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class CronServiceFactoryImpl extends CronServiceFactory {

	@Override
	protected CronService createService(Map<String, Object> properties) {
		Injector injector = Guice.createInjector(new CronProdBindings(properties));
		CronServiceImpl impl = injector.getInstance(CronServiceImpl.class);
		
		Object factoryObj = properties.get(CronServiceFactory.NOSQL_MGR_FACTORY);
		if(factoryObj == null) 
			throw new IllegalArgumentException("NOSQL_MGR_FACTORY is required and must be set");
		else if(!(factoryObj instanceof NoSqlEntityManagerFactory))
			throw new IllegalArgumentException("NOSQL_MGR_FACTORY is not an instance of NoSqlEntityManagerFactory");
		NoSqlEntityManagerFactory factory = (NoSqlEntityManagerFactory) factoryObj;
		impl.setFactory(factory);
		return impl;
	}

}
