package com.alvazan.orm.api.base.spi;

public interface KeyGenerator {

	public Object generateNewKey(Object entity);
	
}
