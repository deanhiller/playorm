package com.alvazan.orm.api.spi.layer2;

import com.alvazan.orm.layer2.nosql.cache.NoSqlSessionFactoryImpl;
import com.google.inject.ImplementedBy;

@ImplementedBy(NoSqlSessionFactoryImpl.class)
public interface NoSqlSessionFactory {

	public NoSqlSession createSession();
	
	@SuppressWarnings("rawtypes")
	public MetaQuery parseQuery(String query);

	/**
	 * For ORM layer that want to use TABLE as the alias instead of typing Entity name which allows
	 * you to refactor code without named queries breaking ;).
	 * 
	 * @param query
	 * @param targetTable
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public MetaQuery newsetupByVisitingTree(String query, String targetTable);
	
}
