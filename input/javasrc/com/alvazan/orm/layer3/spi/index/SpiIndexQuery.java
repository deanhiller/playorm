package com.alvazan.orm.layer3.spi.index;

import java.util.List;

public interface SpiIndexQuery<T> {

	public void setParameter(String parameterName, String value);

	public List<T> getResultList();
	
}
