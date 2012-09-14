package com.alvazan.orm.logging;

import com.alvazan.orm.api.z8spi.Cache;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.RowHolder;

public class EmptyCache implements Cache {

	@Override
	public RowHolder<Row> fromCache(String colFamily, byte[] key) {
		return null;
	}

}
