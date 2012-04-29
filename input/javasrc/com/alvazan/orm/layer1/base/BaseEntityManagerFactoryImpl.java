package com.alvazan.orm.layer1.base;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.Converter;
import com.alvazan.orm.api.NoSqlEntityManager;
import com.alvazan.orm.api.NoSqlEntityManagerFactory;
import com.alvazan.orm.impl.meta.ScannerForField;
import com.google.inject.Provider;
import com.impetus.annovention.ClasspathDiscoverer;
import com.impetus.annovention.Filter;

public class BaseEntityManagerFactoryImpl implements NoSqlEntityManagerFactory {

	private static final Logger log = LoggerFactory.getLogger(BaseEntityManagerFactoryImpl.class);
	
	@Inject
	private Provider<NoSqlEntityManager> entityMgrProvider;
	@Inject
	private MyClassAnnotationDiscoveryListener listener;
	@Inject
	private ScannerForField inspectorField;
	@Inject
	private ClasspathDiscoverer discoverer; 
	private boolean isScanned;
	
	@Override
	public NoSqlEntityManager createEntityManager() {
		if(!isScanned)
			throw new IllegalStateException("Must call scanForEntities first");
		return entityMgrProvider.get();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void setup(Map<Class, Converter> converters, String... packages) {
		if(isScanned)
			throw new IllegalStateException("scanForEntities can only be called once");
		
		inspectorField.setCustomConverters(converters);
		
		List<String> temp = new ArrayList<String>();
		for(String p : packages) {
			temp.add(p);
		}
		log.info("Begin scanning package list="+temp);
		
		//TODO: Fork annovention, it is a very small library AND then copy from
		//http://code.google.com/p/reflections/source/browse/trunk/reflections/src/main/java/org/reflections/util/ClasspathHelper.java?r=103
		//so that we only scan classes that are in a certain package instead of all classes on the classpath!!!!
		discoverer.setFilter(new OurFilter(temp));
        // Add class annotation listener (optional)
        discoverer.addAnnotationListener(listener);
        // Fire it
        discoverer.discover();	
        isScanned = true;
	}

	private static class OurFilter implements Filter {
		private List<String> packages;
		public OurFilter(List<String> packages) {
			this.packages = packages;
		}

		@Override
		public boolean accepts(String filename) {
			if(filename.endsWith(".class"))
				return true;
			return false;
		}
	}
}
