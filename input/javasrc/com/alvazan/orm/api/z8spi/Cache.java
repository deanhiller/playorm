package com.alvazan.orm.api.z8spi;



public interface Cache {

	RowHolder<Row> fromCache(String colFamily, byte[] key);

	void cacheRow(String colFamily, byte[] b, Row value);

}
