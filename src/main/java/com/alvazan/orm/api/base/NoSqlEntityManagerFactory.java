package com.alvazan.orm.api.base;

import java.util.List;

public interface NoSqlEntityManagerFactory {
	
	public NoSqlEntityManager createEntityManager();
	
	@SuppressWarnings("rawtypes")
	void rescan(List<Class> classes, ClassLoader cl);

	/**
	 * Releases the entire pool of connections and disconnects from the nosql store.
	 */
	void close();
}
