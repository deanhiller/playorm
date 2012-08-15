package com.alvazan.orm.impl.meta.scan;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.anno.NoSqlEmbeddable;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlInheritance;
import com.alvazan.orm.api.base.anno.NoSqlManyToMany;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlOneToMany;
import com.alvazan.orm.api.base.anno.NoSqlOneToOne;
import com.alvazan.orm.api.base.anno.NoSqlTransient;
import com.alvazan.orm.api.spi1.meta.DboDatabaseMeta;
import com.alvazan.orm.api.spi1.meta.DboTableMeta;
import com.alvazan.orm.impl.meta.data.MetaAbstractClass;
import com.alvazan.orm.impl.meta.data.MetaClassInheritance;
import com.alvazan.orm.impl.meta.data.MetaClassSingle;
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
		MetaAbstractClass classMeta = metaInfo.findOrCreate(clazz);
		
		NoSqlInheritance annotation = clazz.getAnnotation(NoSqlInheritance.class);
		if(classMeta instanceof MetaClassInheritance) {
			MetaClassInheritance classMeta2 = (MetaClassInheritance) classMeta;
			scanMultipleClasses(annotation, classMeta2);
		} else {
			MetaClassSingle classMeta2 = (MetaClassSingle)classMeta;
			DboTableMeta metaDbo = classMeta2.getMetaDbo();
			scanForAnnotations(classMeta2);
			scanSingle(classMeta2, metaDbo);
		}
		
		if(classMeta.getIdField() == null)
			throw new IllegalArgumentException("Entity="+classMeta.getMetaClass()+" has no field annotated with @NoSqlId and that is required");
		
		metaInfo.addTableNameLookup(classMeta);
		databaseInfo.addMetaClassDbo(classMeta.getMetaDbo());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <T> void scanMultipleClasses(NoSqlInheritance annotation, MetaClassInheritance<T> metaClass) {
		Class<T> mainClass = metaClass.getMetaClass();
		NoSqlEntity noSqlEntity = metaClass.getMetaClass().getAnnotation(NoSqlEntity.class);
		String colFamily = noSqlEntity.columnfamily();
		if("".equals(colFamily))
			colFamily = metaClass.getMetaClass().getSimpleName();
		metaClass.setColumnFamily(colFamily);

		DboTableMeta metaDbo = metaClass.getMetaDbo();
		List<Field> fields = findAllFields(mainClass);
		for(Field field : fields) {
			processIdFieldWorks(metaClass, metaDbo, field);
		}
		
		String discColumn = annotation.discriminatorColumnName();
		metaClass.setDiscriminatorColumnName(discColumn);
		
		for(Class clazz : annotation.subclassesToScan()) {
			MetaClassSingle<?> metaSingle = metaClass.findOrCreate(clazz, mainClass);
			metaSingle.setColumnFamily(metaClass.getColumnFamily());
			metaSingle.setMetaClass(clazz);
			metaInfo.addSubclass(clazz, metaClass);
			scanSingle(metaSingle, metaDbo);
		}
	}

	private <T> void scanSingle(MetaClassSingle<T> classMeta, DboTableMeta metaDbo) {
		Class<? extends T> proxyClass = createTheProxy(classMeta.getMetaClass());
		classMeta.setProxyClass(proxyClass);
		scanFields(classMeta, metaDbo);
	}
	
	@SuppressWarnings("unchecked")
	private <T> Class<T> createTheProxy(Class<?> mainClass) {
		ProxyFactory f = new ProxyFactory();
		f.setSuperclass(mainClass);
		f.setInterfaces(new Class[] {NoSqlProxy.class});
		f.setFilter(new MethodFilter() {
			public boolean isHandled(Method m) {
				// ignore finalize()
				if(m.getName().equals("finalize"))
					return false;
				else if(m.getName().equals("equals"))
					return false;
				else if(m.getName().equals("hashCode"))
					return false;
				return true;
			}
		});
		Class<T> clazz = f.createClass();
		testInstanceCreation(clazz);
		
		return clazz;
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
	
	private void scanForAnnotations(MetaAbstractClass<?> meta) {
		NoSqlEntity noSqlEntity = meta.getMetaClass().getAnnotation(NoSqlEntity.class);
		NoSqlEmbeddable embeddable = meta.getMetaClass().getAnnotation(NoSqlEmbeddable.class);
		if(noSqlEntity != null) {
			String colFamily = noSqlEntity.columnfamily();
			if("".equals(colFamily))
				colFamily = meta.getMetaClass().getSimpleName();
			meta.setColumnFamily(colFamily);
		} else if(embeddable != null) {
			log.trace("nothing to do yet here until we implement");
			//nothing to do at this point
		} else {
			throw new RuntimeException("bug, someone added an annotation but didn't add to this else clause(add else if to this guy)");
		}
	}
	
	private void scanFields(MetaClassSingle<?> meta, DboTableMeta metaDbo) {
		Class<?> metaClass = meta.getMetaClass();
		List<Field> fields = findAllFields(metaClass);
		
		for(Field field : fields) {
			inspectField(meta, metaDbo, field);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void inspectField(MetaClassSingle<?> metaClass, DboTableMeta metaDbo, Field field) {
		if(Modifier.isTransient(field.getModifiers()) || 
				Modifier.isStatic(field.getModifiers()) ||
				field.isAnnotationPresent(NoSqlTransient.class))
			return;
		
		if(processIdFieldWorks(metaClass, metaDbo, field))
			return;
		
		MetaField metaField;
		if(field.isAnnotationPresent(NoSqlManyToOne.class))
			metaField = inspectorField.processManyToOne(metaDbo, field);
		else if(field.isAnnotationPresent(NoSqlOneToOne.class))
			metaField = inspectorField.processOneToOne(metaDbo, field);
		else if(field.isAnnotationPresent(NoSqlManyToMany.class))
			metaField = inspectorField.processManyToMany(metaDbo, field);
		else if(field.isAnnotationPresent(NoSqlOneToMany.class))
			metaField = inspectorField.processOneToMany(metaDbo, field);
		else if(field.isAnnotationPresent(NoSqlEmbeddable.class))
			metaField = inspectorField.processEmbeddable(field);
		else
			metaField = inspectorField.processColumn(metaDbo, field);
		
		metaClass.addMetaField(metaField);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private boolean processIdFieldWorks(MetaAbstractClass metaClass, DboTableMeta metaDbo, Field field) {
		if(!field.isAnnotationPresent(NoSqlId.class))
			return false;
		
		if(metaClass.getIdField() != null)
			throw new IllegalArgumentException("class="+metaClass.getClass()+" has two fields that have @NoSqlId annotation.  One of them may be in a superclass");
		
		MetaIdField idField = inspectorField.processId(metaDbo, field, metaClass);
		metaClass.setIdField(idField);
		return true;
	}

	private List<Field> findAllFields(Class<?> metaClass) {
		List<Field[]> fields = new ArrayList<Field[]>();
		findFields(metaClass, fields);
		
		List<Field> allFields = new ArrayList<Field>();
		for(Field[] f : fields) {
			List<Field> asList = Arrays.asList(f);
			allFields.addAll(asList);
		}
		return allFields;
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
