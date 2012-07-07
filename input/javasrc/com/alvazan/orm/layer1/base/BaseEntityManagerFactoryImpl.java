package com.alvazan.orm.layer1.base;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.Converter;
import com.alvazan.orm.api.NoSqlEntityManager;
import com.alvazan.orm.api.NoSqlEntityManagerFactory;
import com.alvazan.orm.impl.meta.MetaClass;
import com.alvazan.orm.impl.meta.MetaInfo;
import com.alvazan.orm.impl.meta.ScannerForField;
import com.alvazan.orm.impl.meta.ScannerForQuery;
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
	private ScannerForQuery inspectorQuery;
	@Inject
	private ClasspathDiscoverer discoverer; 
	private boolean isScanned;
	@Inject
	private MetaInfo metaInfo;
	
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
        
        if(log.isTraceEnabled()) {
        	URL[] resources = discoverer.findResources();
        	for(URL res : resources) {
        		log.trace("jar="+res);
        	}
        }
        
        Collection<MetaClass> allEntities = metaInfo.getAllEntities();
        for(MetaClass meta : allEntities) {
        	inspectorQuery.setupQueryStuff(meta);
        }
        
        isScanned = true;
	}

	private static class OurFilter implements Filter {
		private List<String> packages;
		public OurFilter(List<String> packages) {
			this.packages = packages;
		}

		@Override
		public boolean accepts(String filename) {
			if(filename.startsWith("com/alvazan"))
				log.info("filename="+filename);
			if(filename.endsWith(".class"))
				return true;
			return false;
		}
	}
}
