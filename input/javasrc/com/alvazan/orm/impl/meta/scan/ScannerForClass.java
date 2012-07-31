package com.alvazan.orm.impl.meta.scan;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.anno.Embeddable;
import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.Indexed;
import com.alvazan.orm.api.base.anno.ManyToMany;
import com.alvazan.orm.api.base.anno.ManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.OneToMany;
import com.alvazan.orm.api.base.anno.OneToOne;
import com.alvazan.orm.api.base.anno.Transient;
import com.alvazan.orm.api.spi2.DboDatabaseMeta;
import com.alvazan.orm.api.spi2.DboTableMeta;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaField;
import com.alvazan.orm.impl.meta.data.MetaIdField;
import com.alvazan.orm.impl.meta.data.MetaInfo;
import com.alvazan.orm.impl.meta.data.NoSqlProxy;

public class ScannerForClass {

	private static final Logger log = LoggerFactory.getLogger(ScannerForClass.class);
	
	@Inject
	private ScannerForField inspectorField;
	@Inject
	private MetaInfo metaInfo;
	@Inject
	private DboDatabaseMeta databaseInfo;
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void addClass(Class<?> clazz) {
		log.info("scanning class="+clazz);
		//NOTE: We scan linearly with NO recursion BUT when we hit an object like Activity.java
		//that has a reference to Account.java and so the MetaClass of Activity has fields and that
		//field needs a reference to the MetaClass of Account.  To solve this, it creates a shell
		//of MetaClass that will be filled in here when Account gets scanned(if it gets scanned
		//after Activity that is).  You can open call heirarchy on findOrCreateMetaClass ;).
		MetaClass classMeta = metaInfo.findOrCreate(clazz);
		classMeta.setMetaClass(clazz);
		createAndSetProxy(classMeta);
		scanClass(classMeta);
		
		metaInfo.addTableNameLookup(classMeta);
		
		databaseInfo.addMetaClassDbo(classMeta.getMetaDbo());
	}

	@SuppressWarnings("unchecked")
	private <T> void createAndSetProxy(MetaClass<T> classMeta) {
		Class<T> fieldType = classMeta.getMetaClass();
		ProxyFactory f = new ProxyFactory();
		f.setSuperclass(fieldType);
		f.setInterfaces(new Class[] {NoSqlProxy.class});
		f.setFilter(new MethodFilter() {
			public boolean isHandled(Method m) {
				// ignore finalize()
				return !m.getName().equals("finalize");
			}
		});
		Class<T> clazz = f.createClass();
		testInstanceCreation(clazz);
		
		classMeta.setProxyClass(clazz);
	}
	
	/**
	 * An early test so we get errors on startup instead of waiting until runtime(a.k.a fail as fast as we can)
	 */
	private Proxy testInstanceCreation(Class<?> clazz) {
		try {
			Proxy inst = (Proxy) clazz.newInstance();
			return inst;
		} catch (InstantiationException e) {
			throw new RuntimeException("Could not create proxy for type="+clazz, e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Could not create proxy for type="+clazz, e);
		}
	}
	
	private void scanClass(MetaClass<?> meta) {
		NoSqlEntity noSqlEntity = meta.getMetaClass().getAnnotation(NoSqlEntity.class);
		Embeddable embeddable = meta.getMetaClass().getAnnotation(Embeddable.class);
		if(noSqlEntity != null) {
			String colFamily = noSqlEntity.columnfamily();
			if("".equals(colFamily))
				colFamily = meta.getMetaClass().getSimpleName();
			meta.setColumnFamily(colFamily);
		} else if(embeddable != null) {
			log.trace("nothing to do yet here until we implement");
			//nothing to do at this point
		} else {
			throw new RuntimeException("bug, someone added an annotation but didn't add an else if here");
		}
		
		scanFields(meta);
	}
	private void scanFields(MetaClass<?> meta) {
		Class<?> metaClass = meta.getMetaClass();
		List<Field[]> fields = new ArrayList<Field[]>();
		findFields(metaClass, fields);
		
		for(Field[] fieldArray : fields) {
			for(Field field : fieldArray) {
				inspectField(meta, field);
			}
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void inspectField(MetaClass<?> metaClass, Field field) {
		if(Modifier.isTransient(field.getModifiers()) || 
				Modifier.isStatic(field.getModifiers()) ||
				field.isAnnotationPresent(Transient.class))
			return;
		
		DboTableMeta metaDbo = metaClass.getMetaDbo();
		if(field.isAnnotationPresent(Id.class)) {
			MetaIdField idField = inspectorField.processId(field, metaClass);
			metaClass.setIdField(idField);
			return;
		}
		
		String cf = metaClass.getColumnFamily();
		MetaField metaField;
		if(field.isAnnotationPresent(ManyToOne.class))
			metaField = inspectorField.processManyToOne(field, cf);
		else if(field.isAnnotationPresent(OneToOne.class))
			metaField = inspectorField.processOneToOne(field, cf);
		else if(field.isAnnotationPresent(ManyToMany.class))
			metaField = inspectorField.processManyToMany(field);
		else if(field.isAnnotationPresent(OneToMany.class))
			metaField = inspectorField.processOneToMany(field);
		else if(field.isAnnotationPresent(Embeddable.class))
			metaField = inspectorField.processEmbeddable(field);
		else
			metaField = inspectorField.processColumn(field, cf);
		
		boolean isIndexed = field.isAnnotationPresent(Indexed.class);
		metaClass.addMetaField(metaField, isIndexed);
		metaDbo.addColumnMeta(metaField.getMetaDbo());
	}
	
	@SuppressWarnings("rawtypes")
	private void findFields(Class metaClass2, List<Field[]> fields) {
		Class next = metaClass2;
		while(true) {
			Field[] f = next.getDeclaredFields();
			fields.add(f);
			next = next.getSuperclass();
			if(next.equals(Object.class))
				return;
		}
	}

}
