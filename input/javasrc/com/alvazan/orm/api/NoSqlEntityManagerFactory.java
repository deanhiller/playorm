package com.alvazan.orm.api;

import com.alvazan.orm.impl.base.BaseEntityManagerFactoryImpl;
import com.google.inject.ImplementedBy;

@ImplementedBy(BaseEntityManagerFactoryImpl.class)
public interface NoSqlEntityManagerFactory {

	
	public void scanForEntities(String ... packages);
	
	public NoSqlEntityManager createEntityManager();
	
}
