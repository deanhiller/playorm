package com.alvazan.orm.layer3.spi.index;

public interface SpiIndexQueryFactory {

	/**
	 * We will call this method EVERY time we want to run a query so that the SpiIndexQuery can have
	 * state and store parameters!!!
	 * @param indexName
	 * @return
	 */
	public SpiIndexQuery createQuery(String indexName);
	
}
