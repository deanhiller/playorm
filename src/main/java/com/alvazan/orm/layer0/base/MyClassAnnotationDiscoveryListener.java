package com.alvazan.orm.layer0.base;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.anno.NoSqlEmbeddable;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.impl.meta.scan.ScannerForClass;
import com.impetus.annovention.listener.ClassAnnotationDiscoveryListener;

public class MyClassAnnotationDiscoveryListener implements
		ClassAnnotationDiscoveryListener {

	private static final Logger log = LoggerFactory.getLogger(MyClassAnnotationDiscoveryListener.class);

	@Inject
	private ScannerForClass inspectorClass;

	private ClassLoader classLoader; 

	@Override
	public String[] supportedAnnotations() {
		return new String[] {NoSqlEntity.class.getName(), NoSqlEmbeddable.class.getName()};
	}

	@Override
	public void discovered(String clazzName, String annotation) {
		if(log.isDebugEnabled())
			log.debug("class="+clazzName+" anno="+annotation);
		try {
			Class<?> clazz = classLoader.loadClass(clazzName);

			scanClass(clazz);
			
			while(clazz.getSuperclass() != java.lang.Object.class) {
				clazz = clazz.getSuperclass();
				inspectorClass.addClassForQueries(clazz);
			}
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public void scanClass(Class<?> clazz) {
		try {
			inspectorClass.addClass(clazz);
		} catch(RuntimeException e) {
			throw new RuntimeException("Failure scanning class(see chained exception)="+clazz, e);
		}
	}

	public void setClassLoader(ClassLoader cl) {
		this.classLoader = cl;
	}

}
