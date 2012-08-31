package com.alvazan.orm.api.spi9.db;


public interface MetaLookup {

	<T> T find(Class<T> class1, Object colFamily);

}
