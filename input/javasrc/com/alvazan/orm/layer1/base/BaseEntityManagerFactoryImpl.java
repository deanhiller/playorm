package com.alvazan.orm.layer1.base;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.Converter;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;
import com.alvazan.orm.api.spi.layer2.MetaQuery;
import com.alvazan.orm.api.spi.layer2.NoSqlSessionFactory;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaInfo;
import com.alvazan.orm.impl.meta.scan.ScannerForField;
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
	private NoSqlSessionFactory noSqlSessionFactory;
	@Inject
	private ClasspathDiscoverer discoverer; 
	private boolean isScanned;
	@Inject
	private MetaInfo metaInfo;

	private Object injector;
	
	@Override
	public NoSqlEntityManager createEntityManager() {
		if(!isScanned)
			throw new IllegalStateException("Must call scanForEntities first");
		return entityMgrProvider.get();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void setup(Map<Class, Converter> converters) {
		if(isScanned)
			throw new IllegalStateException("scanForEntities can only be called once");
		
		inspectorField.setCustomConverters(converters);
		
		log.info("Begin scanning for jars with nosql.Persistence.class");
		
		//TODO: Fork annovention, it is a very small library AND then copy from
		//http://code.google.com/p/reflections/source/browse/trunk/reflections/src/main/java/org/reflections/util/ClasspathHelper.java?r=103
		//so that we only scan classes that are in a certain package instead of all classes on the classpath!!!!
		discoverer.setFilter(new OurFilter());
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
        	setupQueryStuff(meta);
        }
        
        log.info("Finished scanning classes");
        isScanned = true;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void setupQueryStuff(MetaClass classMeta) {
		Class<?> clazz = classMeta.getMetaClass();
		NoSqlQuery annotation = clazz.getAnnotation(NoSqlQuery.class);
		NoSqlQueries annotation2 = clazz.getAnnotation(NoSqlQueries.class);
		List<NoSqlQuery> theQueries = new ArrayList<NoSqlQuery>();
		if(annotation2 != null) {
			NoSqlQuery[] queries = annotation2.value();
			List<NoSqlQuery> asList = Arrays.asList(queries);
			theQueries.addAll(asList);
		}
		if(annotation != null)
			theQueries.add(annotation);

		log.info("Parsing queries for entity="+classMeta.getMetaClass());
		for(NoSqlQuery query : theQueries) {
			log.info("parsing query="+query.name()+" query="+query.query());
			MetaQuery metaQuery = createQueryAndAdd(classMeta, query);
			classMeta.addQuery(query.name(), metaQuery);
		}
	}

	@SuppressWarnings("rawtypes")
	private MetaQuery createQueryAndAdd(MetaClass classMeta, NoSqlQuery query) {
		try {
			// parse and setup this query once here to be used by ALL of the
			// SpiIndexQuery objects.
			// NOTE: This is meta data to be re-used by all threads and all
			// instances of query objects only!!!!

			// We must walk the tree allowing 2 visitors to see it.
			// The first visitor would be ourselves maybe? to get all parameter info
			// The second visitor is the SPI Index so it can create it's "prototype"
			// query (prototype pattern)
			MetaQuery metaQuery = noSqlSessionFactory.parseQueryForOrm(query.query(), classMeta.getColumnFamily());

			return metaQuery;
		} catch(RuntimeException e) {
			throw new RuntimeException("Named query="+query.name()+" on class="
					+classMeta.getMetaClass()+" failed to parse.  query=\""+query.query()
					+"\"  See chained exception for cause", e);
		}
	}
	
	private static class OurFilter implements Filter {
		@Override
		public boolean accepts(String filename) {
			if(filename.endsWith(".class"))
				return true;
			return false;
		}
	}

	public Object getInjector() {
		return injector;
	}

	public void setInjector(Object injector) {
		this.injector = injector;
	}

}
