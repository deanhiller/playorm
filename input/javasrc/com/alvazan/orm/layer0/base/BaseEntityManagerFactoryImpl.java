package com.alvazan.orm.layer0.base;

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

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;
import com.alvazan.orm.api.spi2.ColumnTypeEnum;
import com.alvazan.orm.api.spi2.DboColumnMeta;
import com.alvazan.orm.api.spi2.DboDatabaseMeta;
import com.alvazan.orm.api.spi2.DboTableMeta;
import com.alvazan.orm.api.spi2.MetaQuery;
import com.alvazan.orm.api.spi2.NoSqlSessionFactory;
import com.alvazan.orm.api.spi2.StorageTypeEnum;
import com.alvazan.orm.api.spi3.db.conv.Converter;
import com.alvazan.orm.impl.meta.data.MetaAbstractClass;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaInfo;
import com.alvazan.orm.impl.meta.scan.ScannerForField;
import com.impetus.annovention.ClasspathDiscoverer;
import com.impetus.annovention.Filter;

public class BaseEntityManagerFactoryImpl implements NoSqlEntityManagerFactory {

	private static final Logger log = LoggerFactory.getLogger(BaseEntityManagerFactoryImpl.class);
	
	@Inject
	private Provider<BaseEntityManagerImpl> entityMgrProvider;
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
	@Inject
	private DboDatabaseMeta databaseInfo;
	
	private Object injector;

	@Override
	public NoSqlEntityManager createEntityManager() {
		if(!isScanned)
			throw new IllegalStateException("Must call scanForEntities first");
		BaseEntityManagerImpl mgr = entityMgrProvider.get();
		mgr.setup();
		return mgr;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void rescan(List<Class> classesToScan, ClassLoader cl) {
		List<Class> classes = classesToScan;
		if(classes == null)
			classes = new ArrayList<Class>();
		metaInfo.clearAll();
		
		listener.setClassLoader(cl);
		
		//TODO: Fork annovention, it is a very small library AND then copy from
		//http://code.google.com/p/reflections/source/browse/trunk/reflections/src/main/java/org/reflections/util/ClasspathHelper.java?r=103
		//so that we only scan classes that are in a certain package instead of all classes on the classpath!!!!
		discoverer.setFilter(new OurFilter());
        // Add class annotation listener (optional)
        discoverer.addAnnotationListener(listener);
        // Fire it
        discoverer.discover(cl);
        
        for(Class c : classes) {
        	listener.scanClass(c);
        }
        
        if(log.isTraceEnabled()) {
        	URL[] resources = discoverer.findResources(cl);
        	for(URL res : resources) {
        		log.trace("jar="+res);
        	}
        }
        
        Collection<MetaAbstractClass> allEntities = metaInfo.getAllEntities();
        for(MetaAbstractClass meta : allEntities) {
        	setupQueryStuff(meta);
        }
        
        log.info("Finished scanning classes, saving meta data");
        isScanned = true;
        
        NoSqlEntityManager tempMgr = createEntityManager();

        saveMetaData(tempMgr);
        
        tempMgr.flush();
        log.info("Finished saving meta data, complelety done initializing");
	}
	
	@SuppressWarnings("rawtypes")
	public void setup(Map<String, Object> properties, Map<Class, Converter> converters, ClassLoader cl) {
		if(isScanned)
			throw new IllegalStateException("scanForEntities can only be called once");
		else if(properties == null)
			throw new IllegalArgumentException("'properties' parameter must be supplied");
		
		String val = (String) properties.get(Bootstrap.AUTO_CREATE_KEY);
		if(val == null)
			throw new IllegalArgumentException("Must provide property with key NoSqlEntityManagerFactory.AUTO_CREATE_KEY so we know to update or validate existing schema");
		AutoCreateEnum autoCreate = AutoCreateEnum.translate(val);
		if(autoCreate == null)
			throw new IllegalArgumentException("Property NoSqlEntityManagerFactory.AUTO_CREATE_KEY can only have values validate,update, or create");
		
		inspectorField.setCustomConverters(converters);
		
		log.info("Begin scanning for jars with nosql.Persistence.class");
		
        List<Class> classToScan = (List<Class>) properties.get(Bootstrap.LIST_OF_EXTRA_CLASSES_TO_SCAN_KEY);
        
        if(AutoCreateEnum.CREATE_ONLY != autoCreate)
        	throw new UnsupportedOperationException("not implemented yet");
        
		rescan(classToScan, cl);
	}

	private void saveMetaData(NoSqlEntityManager tempMgr) {
        DboDatabaseMeta existing = tempMgr.find(DboDatabaseMeta.class, DboDatabaseMeta.META_DB_ROWKEY);
//        if(existing != null)
//        	throw new IllegalStateException("Your property NoSqlEntityManagerFactory.AUTO_CREATE_KEY is set to 'create' which only creates meta data if none exist already but meta already exists");
		
        for(DboTableMeta table : databaseInfo.getAllTables()) {
        	
        	for(DboColumnMeta col : table.getAllColumns()) {
        		tempMgr.put(col);
        	}
        	
        	tempMgr.put(table.getIdColumnMeta());
        	
        	tempMgr.put(table);
        }
        
        databaseInfo.setId(DboDatabaseMeta.META_DB_ROWKEY);
        
        //NOW, on top of the ORM entites, we have 3 special index column families of String, BigInteger and BigDecimal
        //which are one of the types in the composite column name.(the row keys are all strings).  The column names
        //are <value being indexed of String or BigInteger or BigDecimal><primarykey><length of first value> so we can
        //sort it BUT we can determine the length of first value so we can get to primary key.
        
        for(StorageTypeEnum type : StorageTypeEnum.values()) {
        	if(type == StorageTypeEnum.BYTES)
        		continue;
        	DboTableMeta cf = new DboTableMeta();
        	DboColumnMeta idMeta = new DboColumnMeta();
        	idMeta.setup("id", null, String.class, ColumnTypeEnum.ID, null);
        	
        	cf.setColumnFamily(type.getIndexTableName());
        	cf.setColNamePrefixType(type);
        	cf.setRowKeyMeta(idMeta);
        	
        	tempMgr.put(idMeta);
        	tempMgr.put(cf);
        	
        	databaseInfo.addMetaClassDbo(cf);
        }
        
        tempMgr.put(databaseInfo);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void setupQueryStuff(MetaAbstractClass classMeta) {
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

	@Override
	public void close() {
		this.noSqlSessionFactory.close();
	}

}
