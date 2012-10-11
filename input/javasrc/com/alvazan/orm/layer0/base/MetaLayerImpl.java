package com.alvazan.orm.layer0.base;

import javax.inject.Inject;

import com.alvazan.orm.api.base.MetaLayer;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.impl.meta.data.MetaClass;
import com.alvazan.orm.impl.meta.data.MetaInfo;

@SuppressWarnings("rawtypes")
public class MetaLayerImpl implements MetaLayer {

	@Inject
	private MetaInfo metaInfo;
	
	@SuppressWarnings({ "unchecked" })
	@Override
	public Object getKey(Object entity) {
		MetaClass metaClass = getMetaClass(entity.getClass());
		return metaClass.fetchId(entity);
	}

	private MetaClass getMetaClass(Class type) {
		MetaClass meta = metaInfo.getMetaClass(type);
		if(meta == null)
			throw new IllegalArgumentException("Meta information not found on class="+type.getSimpleName());
		return meta;
	}
	
	@Override
	public boolean isManagedEntity(Class<?> type) {
		MetaClass metaClass = metaInfo.getMetaClass(type);
		return metaClass != null;
	}
	
	@Override
	public String getKeyFieldName(Class<?> type) {
		MetaClass meta = getMetaClass(type);
		return meta.getIdField().getFieldName();
	}

	@Override
	public Object convertIdFromString(Class<?> entityType, String idAsString) {
		MetaClass meta = getMetaClass(entityType);
		DboColumnIdMeta idMeta = meta.getIdField().getMetaIdDbo();
		return idMeta.convertStringToType(idAsString);
	}

}
