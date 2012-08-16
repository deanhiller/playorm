package com.alvazan.orm.api.spi5;

import java.util.List;

public interface SpiQueryAdapter {

	public void setParameter(String parameterName, byte[] value);

	@SuppressWarnings("rawtypes")
	public List getResultList();

	public void setFirstResult(int firstResult);

	public void setMaxResults(int batchSize);
	
}
