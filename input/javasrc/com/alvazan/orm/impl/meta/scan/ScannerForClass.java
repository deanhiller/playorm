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

import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.ManyToMany;
import com.alvazan.orm.api.base.anno.ManyToOne;
import com.alvazan.orm.api.base.anno.NoDiscriminatorColumn;
import com.alvazan.orm.api.base.anno.NoSqlEmbeddable;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlInheritance;
import com.alvazan.orm.api.base.anno.NoSqlTransient;
import com.alvazan.orm.api.base.anno.OneToMany;
import com.alvazan.orm.api.base.anno.OneToOne;
import com.alvazan.orm.api.spi2.DboDatabaseMeta;
import com.alvazan.orm.api.spi2.DboTableMeta;
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
			scanMultipleClasses(annotation, classMeta2, clazz);
		} else {
			MetaClassSingle classMeta2 = (MetaClassSingle)classMeta;
			Class proxyClass = createTheProxy(clazz);
			classMeta2.setProxyClass(proxyClass);
			metaInfo.addTableNameLookup(classMeta);
			scanClass(classMeta);
			databaseInfo.addMetaClassDbo(classMeta.getMetaDbo());
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void scanMultipleClasses(NoSqlInheritance annotation, MetaClassInheritance metaClass, Class<?> mainClass) {
		for(Class<?> clazz : annotation.columnfamily()) {
			NoDiscriminatorColumn col = clazz.getAnnotation(NoDiscriminatorColumn.class);
			if(col == null)
				throw new IllegalArgumentException("Class "+mainClass.getName()+" in the NoSqlInheritance annotation, specifies a class" +
						" that is missing the NoSqlDiscriminatorColumn annotation.  Class to add annotation to="+clazz.getName());
			String columnValue = col.value();
			Class proxyClass = createTheProxy(mainClass);
			
			metaClass.addProxy(columnValue, proxyClass);
		
			//throw new UnsupportedOperationException("not done yet");
			//scanClass(classMeta);
		}
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
	
	private void scanClass(MetaAbstractClass<?> meta) {
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
			throw new RuntimeException("bug, someone added an annotation but didn't add an else if here");
		}
		
		scanFields(meta);
	}
	private void scanFields(MetaAbstractClass<?> meta) {
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
	private void inspectField(MetaAbstractClass<?> metaClass, Field field) {
		if(Modifier.isTransient(field.getModifiers()) || 
				Modifier.isStatic(field.getModifiers()) ||
				field.isAnnotationPresent(NoSqlTransient.class))
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
		else if(field.isAnnotationPresent(NoSqlEmbeddable.class))
			metaField = inspectorField.processEmbeddable(field);
		else
			metaField = inspectorField.processColumn(field, cf);
		
		boolean isIndexed = field.isAnnotationPresent(NoSqlIndexed.class);
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
