package com.alvazan.orm.api.z8spi;


public interface MetaLookup {

	<T> T find(Class<T> class1, Object colFamily);

}
