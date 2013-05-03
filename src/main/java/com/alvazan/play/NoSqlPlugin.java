package com.alvazan.play;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import play.Play;
import play.PlayPlugin;
import play.data.binding.ParamNode;
import play.data.binding.RootParamNode;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.MetaLayer;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.exc.RowNotFoundException;
import com.alvazan.play.logging.CassandraAppender;

public class NoSqlPlugin extends PlayPlugin {

	private static final Logger log = LoggerFactory.getLogger(NoSqlPlugin.class);

	private static final Map<String, Object> PROPS = new HashMap<String, Object>();

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
    	if(o == null)
    		throw new RowNotFoundException("Row with rowkey="+theId+" was not found, but your page posted this id to lookup the row of class type="+clazz.getSimpleName());
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
        List<Class> classes = Play.classloader.getAnnotatedClasses(NoSqlEntity.class);
        if (classes.isEmpty())
            return;
        else if (NoSql.getEntityManagerFactory() != null) {
        	NoSqlEntityManagerFactory factory = NoSql.getEntityManagerFactory();
        	factory.rescan(classes, Play.classloader);
        	return;
        }

		PROPS.put(Bootstrap.LIST_OF_EXTRA_CLASSES_TO_SCAN_KEY, classes);
		PROPS.put(Bootstrap.AUTO_CREATE_KEY, "create");

		for (java.util.Map.Entry<Object, Object> entry : Play.configuration.entrySet()) {
			PROPS.put((String) entry.getKey(), entry.getValue());
		}
        
        if (log.isInfoEnabled())
			log.info("Initializing PlayORM...");

        NoSqlEntityManagerFactory factory = Bootstrap.create(PROPS, Play.classloader);
        NoSql.setEntityManagerFactory(factory);
        CassandraAppender.setFactory(factory);
	}

	@Override
	public void onApplicationStop() {
		if (log.isInfoEnabled())
			log.info("stopping PlayOrm");
	}
	
    @Override
    public void beforeInvocation() {
        if (!NoSql.isEnabled())
            return;

        NoSqlEntityManager manager = NoSql.getEntityManagerFactory().createEntityManager();
        NoSql.createContext(manager);
    }
    
    @Override
    public void afterInvocation() {
    	NoSql.clearContext();
    }

    @Override
    public void onInvocationException(Throwable e) {
    	NoSql.clearContext();
    }

    @Override
    public void invocationFinally() {
    	NoSql.clearContext();
    }

	public static Map<String, Object> getProperties() {
		return PROPS;
	}
}
