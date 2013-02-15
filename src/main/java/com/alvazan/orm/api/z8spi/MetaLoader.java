package com.alvazan.orm.api.z8spi;


public interface MetaLoader {

	<T> T find(Class<T> class1, Object id);

}
