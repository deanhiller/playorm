package com.alvazan.orm.api.spi5;

import com.alvazan.orm.api.spi9.db.IndexColumn;


public interface SpiQueryAdapter {

	public void setParameter(String parameterName, byte[] value);

	public Iterable<IndexColumn> getResultList();

	public void setFirstResult(int firstResult);

	public void setMaxResults(int batchSize);
	
}
