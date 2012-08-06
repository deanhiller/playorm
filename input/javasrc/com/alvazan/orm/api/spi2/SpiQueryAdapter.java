package com.alvazan.orm.api.spi2;

import java.util.List;

public interface SpiQueryAdapter {

	public void setParameter(String parameterName, byte[] value);

	@SuppressWarnings("rawtypes")
	public List getResultList();
	
}
