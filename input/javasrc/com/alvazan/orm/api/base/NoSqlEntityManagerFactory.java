package com.alvazan.orm.api.base;

import java.util.List;

import com.alvazan.orm.layer0.base.BaseEntityManagerFactoryImpl;
import com.google.inject.ImplementedBy;

@ImplementedBy(BaseEntityManagerFactoryImpl.class)
public interface NoSqlEntityManagerFactory {
	
	public NoSqlEntityManager createEntityManager();
	
	@SuppressWarnings("rawtypes")
	void rescan(List<Class> classes, ClassLoader cl);

	/**
	 * Releases the entire pool of connections and disconnects from the nosql store.
	 */
	void close();
}
