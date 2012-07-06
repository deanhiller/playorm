package com.alvazan.orm.layer3.spi.index;

import com.alvazan.orm.impl.meta.MetaClass;
import com.alvazan.orm.impl.meta.MetaQuery;

public interface SpiIndexQueryFactory<T> {

	public void setup(MetaClass<T> metaClass, MetaQuery<T> metaQuery);
	
	/**
	 * We will call this method EVERY time we want to run a query so that the SpiIndexQuery can have
	 * state and store parameters!!!
	 * @param indexName
	 * @return
	 */
	public SpiIndexQuery<T> createQuery(String indexName);
	
}
