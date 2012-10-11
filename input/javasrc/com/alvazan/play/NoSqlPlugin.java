package com.alvazan.play;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import play.Play;
import play.PlayPlugin;
import play.data.binding.ParamNode;
import play.data.binding.RootParamNode;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.MetaLayer;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.base.anno.NoSqlEntity;

public class NoSqlPlugin extends PlayPlugin {

	private static final Logger log = LoggerFactory.getLogger(NoSqlPlugin.class);
	
    @SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
    public Object bind(RootParamNode rootParamNode, String name, Class clazz, java.lang.reflect.Type type, Annotation[] annotations) {
        NoSqlEntityManager em = NoSql2.em();
        MetaLayer metaLayer = em.getMeta();
        if(!metaLayer.isManagedEntity(clazz))
        	return null;
        
        ParamNode paramNode = rootParamNode.getChild(name, true);

        String keyFieldName = metaLayer.getKeyFieldName(clazz);
        ParamNode id = paramNode.getChild(keyFieldName);
        
        if(id == null) {
        	return NoSqlModel.create(rootParamNode, name, clazz, annotations);
        }

        String[] ids = id.getValues();
        if(ids == null || ids.length == 0)
        	return NoSqlModel.create(rootParamNode, name, clazz, annotations);
        
        String idStr = ids[0];
        Object theId = metaLayer.convertIdFromString(clazz, idStr);
        
        //Read the entity in so that this entity is used instead...
    	Object o = em.find(clazz, theId);
    	
    	return NoSqlModel.edit(rootParamNode, name, o, annotations);
    }

    @Override
    public Object bindBean(RootParamNode rootParamNode, String name, Object bean) {
    	NoSqlEntityManager mgr = NoSql2.em();
    	MetaLayer meta = mgr.getMeta();
    	if(meta.isManagedEntity(bean.getClass())) {
            return NoSqlModel.edit(rootParamNode, name, bean, null);
        }
        return null;
    }
    
	@SuppressWarnings("rawtypes")
	@Override
	public void onApplicationStart() {
        if (NoSql2.getEntityManagerFactory() != null)
        	return;
        
        List<Class> classes = Play.classloader.getAnnotatedClasses(NoSqlEntity.class);
        if (classes.isEmpty())
            return;

        String prop = Play.configuration.getProperty("nosql.db");
        if(StringUtils.isEmpty(prop)) 
        	throw new IllegalArgumentException("nosql.db property must be defined");

		DbTypeEnum type = DbTypeEnum.IN_MEMORY;
		if("cassandra".equalsIgnoreCase(prop)) {
			String clusterName = Play.configuration.getProperty("nosql.cassandra.clustername");
			String keyspace = Play.configuration.getProperty("nosql.cassandra.keyspace");
			String seeds = Play.configuration.getProperty("nosql.cassandra.seeds");
			if(clusterName == null)
				throw new IllegalArgumentException("property nosql.cassandra.clustername is required if using cassandra");
			else if(keyspace == null)
				throw new IllegalArgumentException("property nosql.cassandra.keyspace is required if using cassandra");
			else if(seeds == null)
				throw new IllegalArgumentException("property nosql.cassandra.seeds is required if using cassandra");
			type = DbTypeEnum.CASSANDRA;
		}
		
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(Bootstrap.LIST_OF_EXTRA_CLASSES_TO_SCAN_KEY, classes);

        log.info("Initializing PlayORM...");

        NoSqlEntityManagerFactory factory = Bootstrap.create(type, props, null, Play.classloader);
        NoSql2.setEntityManagerFactory(factory);
	}

	@Override
	public void onApplicationStop() {
		if(NoSql2.getEntityManagerFactory() == null)
			return;
		
		NoSql2.getEntityManagerFactory().close();
		NoSql2.setEntityManagerFactory(null);
	}
	
    @Override
    public void beforeInvocation() {
        if (!NoSql2.isEnabled())
            return;

        NoSqlEntityManager manager = NoSql2.getEntityManagerFactory().createEntityManager();
        NoSql2.createContext(manager);
    }
    
}
