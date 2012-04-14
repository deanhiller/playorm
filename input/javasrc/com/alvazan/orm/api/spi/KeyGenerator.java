package com.alvazan.orm.api.spi;

public interface KeyGenerator {

	public Object generateNewKey(Object entity);
	
}
