package com.alvazan.orm.api.base;

import java.util.List;

import com.alvazan.orm.layer0.base.BaseEntityManagerFactoryImpl;
import com.google.inject.ImplementedBy;

@ImplementedBy(BaseEntityManagerFactoryImpl.class)
public interface NoSqlEntityManagerFactory {
	
	public String AUTO_CREATE_KEY = "autoCreateKey"; 
	public String LIST_OF_EXTRA_CLASSES_TO_SCAN_KEY = "listOfClassesToScan";
	
	public NoSqlEntityManager createEntityManager();
	
	void rescan(List<Class> classes, ClassLoader cl);

}
