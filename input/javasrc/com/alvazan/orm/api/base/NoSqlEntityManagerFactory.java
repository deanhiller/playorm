package com.alvazan.orm.api.base;

import com.alvazan.orm.layer0.base.BaseEntityManagerFactoryImpl;
import com.google.inject.ImplementedBy;

@ImplementedBy(BaseEntityManagerFactoryImpl.class)
public interface NoSqlEntityManagerFactory {
	
	public String AUTO_CREATE_KEY = "autoCreateKey"; 
	
	public NoSqlEntityManager createEntityManager();

}
