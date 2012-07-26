package com.alvazan.orm.api.spi3.index;

import java.util.List;

public interface SpiQueryAdapter {

	public void setParameter(String parameterName, Object value);

	@SuppressWarnings("rawtypes")
	public List getResultList();
	
}
