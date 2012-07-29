package com.alvazan.orm.impl.meta.data;

import com.alvazan.orm.api.spi3.db.Column;

public class ColumnCache {

	public ColumnCache(String name, Column col) {
		this.name = name;
		this.col = col;
	}

}
