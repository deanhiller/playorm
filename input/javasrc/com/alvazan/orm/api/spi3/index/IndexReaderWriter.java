package com.alvazan.orm.api.spi3.index;



public interface IndexReaderWriter {
	
	/**
	 * On startup, we will add named queries by calling this method so you can return us
	 * a factory.  This factory returns us a Query object we will set parameters on to
	 * run the query
	 * @param classMeta 
	 */
	SpiMetaQuery createQueryFactory();

}
