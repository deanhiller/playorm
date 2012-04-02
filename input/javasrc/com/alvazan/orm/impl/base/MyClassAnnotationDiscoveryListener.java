package com.alvazan.orm.impl.base;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.anno.Embeddable;
import com.alvazan.orm.api.anno.NoSqlEntity;
import com.alvazan.orm.impl.meta.InspectorClass;
import com.impetus.annovention.listener.ClassAnnotationDiscoveryListener;

public class MyClassAnnotationDiscoveryListener implements
		ClassAnnotationDiscoveryListener {

	private static final Logger log = LoggerFactory.getLogger(MyClassAnnotationDiscoveryListener.class);

	@Inject
	private InspectorClass inspectorClass; 

	@Override
	public String[] supportedAnnotations() {
		return new String[] {NoSqlEntity.class.getName(), Embeddable.class.getName()};
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void discovered(String clazzName, String annotation) {
		log.info("class="+clazzName+" anno="+annotation);
		try {
			Class clazz = Class.forName(clazzName);
			
			
			inspectorClass.addClass(clazz);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

}
