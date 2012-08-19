package com.alvazan.orm.api.spi5;

import java.util.List;

public interface SpiQueryAdapter {

	public void setParameter(String parameterName, byte[] value);

	public List<byte[]> getResultList();

	public void setFirstResult(int firstResult);

	public void setMaxResults(int batchSize);
	
}
