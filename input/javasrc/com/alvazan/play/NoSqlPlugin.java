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
        NoSqlEntityManager em = NoSql.em();
        MetaLayer metaLayer = em.getMeta();
        if(!metaLayer.isManagedEntity(clazz))
        	return null;
        
        ParamNode paramNode = rootParamNode.getChild(name, true);

        String keyFieldName = metaLayer.getKeyFieldName(clazz);
        ParamNode id = paramNode.getChild(keyFieldName);

        String idStr = NoSqlModel.retrieveValue(id);
        if(idStr == null)
        	return NoSqlModel.create(rootParamNode, name, clazz, annotations);

        Object theId = metaLayer.convertIdFromString(clazz, idStr);
        
        //Read the entity in so that this entity is used instead...
    	Object o = em.find(clazz, theId);
    	
    	return NoSqlModel.edit(rootParamNode, name, o, annotations);
    }

	@Override
    public Object bindBean(RootParamNode rootParamNode, String name, Object bean) {
    	NoSqlEntityManager mgr = NoSql.em();
    	MetaLayer meta = mgr.getMeta();
    	if(meta.isManagedEntity(bean.getClass())) {
            return NoSqlModel.edit(rootParamNode, name, bean, null);
        }
        return null;
    }
    
	@SuppressWarnings("rawtypes")
	@Override
	public void onApplicationStart() {
        if (NoSql.getEntityManagerFactory() != null)
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
        props.put(Bootstrap.AUTO_CREATE_KEY, "create");
        
        log.info("Initializing PlayORM...");

        NoSqlEntityManagerFactory factory = Bootstrap.create(type, props, null, Play.classloader);
        NoSql.setEntityManagerFactory(factory);
	}

	@Override
	public void onApplicationStop() {
		if(NoSql.getEntityManagerFactory() == null)
			return;
		
		NoSql.getEntityManagerFactory().close();
		NoSql.setEntityManagerFactory(null);
	}
	
    @Override
    public void beforeInvocation() {
        if (!NoSql.isEnabled())
            return;

        NoSqlEntityManager manager = NoSql.getEntityManagerFactory().createEntityManager();
        NoSql.createContext(manager);
    }
}
