package com.alvazan.orm.impl.meta;

import java.lang.reflect.Field;

import javax.inject.Inject;

import com.alvazan.orm.api.anno.Column;
import com.alvazan.orm.api.anno.Id;
import com.alvazan.orm.api.spi.KeyGenerator;
import com.google.inject.Provider;

public class InspectorField {

	@Inject
	private Provider<MetaIdField> idMetaProvider;
	@Inject
	private Provider<MetaField> metaProvider;
	
	public MetaIdField processId(Field field) {
		if(!String.class.isAssignableFrom(field.getType()))
			throw new IllegalArgumentException("The id is not of type String and has to be.  field="+field+" in class="+field.getDeclaringClass());
		
		Id idAnno = field.getAnnotation(Id.class);
		MetaIdField metaField = idMetaProvider.get();
		KeyGenerator gen = null;
		if(idAnno.usegenerator()) {
			Class<? extends KeyGenerator> generation = idAnno.generation();
			gen = ReflectionUtil.create(generation);
		}
		
		metaField.setup(field, !idAnno.usegenerator(), gen);
		
		return metaField;
	}

	public MetaField processColumn(Field field) {
		Column col = field.getAnnotation(Column.class);
		MetaField metaField = metaProvider.get();
		String colName = field.getName();
		if(col != null) {
			if(!"".equals(col.columnname()))
				colName = col.columnname();
		}
		
		metaField.setup(field, colName);
		return metaField;
	}
	
	public MetaField processToOne(Field field) {
		// TODO Auto-generated method stub
		return null;
	}

	public MetaField processOneToMany(Field field) {
		// TODO Auto-generated method stub
		return null;
	}

	public MetaField processEmbeddable(Field field) {
		// TODO Auto-generated method stub
		return null;
	}

}
