package com.alvazan.orm.api.base;

import java.util.Map;

import com.alvazan.orm.layer1.base.BaseEntityManagerFactoryImpl;
import com.google.inject.ImplementedBy;

@ImplementedBy(BaseEntityManagerFactoryImpl.class)
public interface NoSqlEntityManagerFactory {
	/**
	 * setup scans all packages if none specified (ALL jars!!! yuck) or if packages
	 * are specified, it scans only the jars with those packages.  The converters is
	 * a Map of converters you can supply.  When a field is found, we first check
	 * if the field has it's own converter then check this Map below for converters
	 * then we check in the standard converters Map which has converters for all
	 * primitives, etc. etc.
	 * 
	 * @param converters
	 * @param packages
	 */
	@SuppressWarnings("rawtypes")
	public void setup(Map<Class, Converter> converters, String ... packages);
	
	public NoSqlEntityManager createEntityManager();
	
}
