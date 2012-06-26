package com.alvazan.orm.layer3.spi.index.inmemory;

import java.util.List;

import com.alvazan.orm.api.TypeMismatchException;
import com.alvazan.orm.impl.meta.MetaField;
import com.alvazan.orm.layer3.spi.index.SpiIndexQuery;
import com.alvazan.orm.layer3.spi.index.SpiQueryInfo;

public class SpiIndexQueryImpl implements SpiIndexQuery {

	private SpiQueryInfo info;

	@Override
	@SuppressWarnings("rawtypes")
	public void setParameter(String paraMeterName, Object value) {
		//check parameter 
		MetaField metaField = info.getMetaFieldByParameter(paraMeterName);
		if(metaField==null){
			throw new IllegalArgumentException("paraMeterName [" + paraMeterName
					+ "] is not find for ");
		}
		Class fieldType = metaField.getFieldType();
		
		if(!fieldType.isAssignableFrom(value.getClass())){
			throw new TypeMismatchException("value [" + value
					+ "] is not match for paraMeterName which should be ["
					+ fieldType + "]");
		} 

	}

	@Override
	public List getResultList() {
		
		return null;
	}

	public void setInfo(SpiQueryInfo info) {
		this.info = info;
	}
}
