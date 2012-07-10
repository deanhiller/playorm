package com.alvazan.orm.api.spi.layer2;

import java.util.List;

import com.alvazan.orm.api.spi.db.Row;
import com.alvazan.orm.layer2.nosql.cache.NoSqlSessionFactoryImpl;
import com.google.inject.ImplementedBy;

@ImplementedBy(NoSqlSessionFactoryImpl.class)
public interface NoSqlSessionFactory {

	public NoSqlSession createSession();
	
	public List<Row> runQuery(String query);

	/**
	 * For ORM layer that want to use TABLE as the alias instead of typing Entity name which allows
	 * you to refactor code without named queries breaking ;).
	 * 
	 * @param query
	 * @param targetTable
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public MetaQuery parseQueryForOrm(String query, String targetTable);
	
}
