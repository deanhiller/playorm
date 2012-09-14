package com.alvazan.orm.api.z8spi;

import com.alvazan.orm.layer5.nosql.cache.RowHolder;

public interface Cache {

	RowHolder<Row> fromCache(String colFamily, byte[] key);

}
