package com.alvazan.orm.impl.meta.data;

import com.alvazan.orm.api.spi.db.Row;

public interface NoSqlProxy {

	void __injectData(Row row);

}
