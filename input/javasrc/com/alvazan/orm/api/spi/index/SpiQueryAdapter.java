package com.alvazan.orm.api.spi.index;

import java.util.List;

public interface SpiQueryAdapter {

	public void setParameter(String parameterName, String value);

	@SuppressWarnings("rawtypes")
	public List getResultList();
	
}
