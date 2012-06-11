package com.alvazan.orm.layer3.spi.index;

import java.util.List;

public interface SpiIndexQuery {

	public void setParameter(String parameterName, Object value);

	@SuppressWarnings("rawtypes")
	public List getResultList();
	
}
