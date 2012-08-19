package com.alvazan.orm.api.spi5;


public interface SpiQueryAdapter {

	public void setParameter(String parameterName, byte[] value);

	public Iterable<byte[]> getResultList();

	public void setFirstResult(int firstResult);

	public void setMaxResults(int batchSize);
	
}
