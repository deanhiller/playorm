package com.alvazan.orm.api.spi.layer2;

import com.alvazan.orm.layer2.nosql.cache.NoSqlSessionFactoryImpl;
import com.google.inject.ImplementedBy;

@ImplementedBy(NoSqlSessionFactoryImpl.class)
public interface NoSqlSessionFactory {

	public NoSqlSession createSession();
	
	
}
