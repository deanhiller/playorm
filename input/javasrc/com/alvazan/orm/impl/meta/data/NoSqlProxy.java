package com.alvazan.orm.impl.meta.data;

import com.alvazan.orm.api.spi3.db.Row;

public interface NoSqlProxy {

	void __injectData(Row row);

}
