package com.alvazan.orm.api.spi;

public interface KeyGenerator {

	public String generateNewKey(Object entity);
	
}
