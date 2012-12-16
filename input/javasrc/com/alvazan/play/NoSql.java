package com.alvazan.play;


import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;

public class NoSql {

    private static NoSqlEntityManagerFactory entityManagerFactory = null;
    private static ThreadLocal<NoSql> local = new ThreadLocal<NoSql>();
    private NoSqlEntityManager entityManager;
    private static boolean plugin2InClassPath = false;

    static {
    	try {
    		Class<?> c = Class.forName("com.alvazan.play2.Play2Plugin");
    		if (c != null) {
    			plugin2InClassPath = true;
    		}
    	} catch(ClassNotFoundException e) {
    		plugin2InClassPath = false;
        }
    }

    static NoSql get() {
        if (local.get() == null) {
            throw new RuntimeException("The Playorm context is not initialized. NoSqlEntityManager automatically start when one or more classes annotated with the @NoSqlEntity annotation are found in the application.");
        }
        return local.get();
    }

    static void clearContext() {
        local.remove();
    }

    static void createContext(NoSqlEntityManager entityManager) {
        if (local.get() != null) {
//            try {
//                local.get().entityManager.close();
//            } catch (Exception e) {
//                // Let's it fail
//            }
            local.remove();
        }
        NoSql context = new NoSql();
        context.entityManager = entityManager;
        local.set(context);
    }

    // ~~~~~~~~~~~
    /*
     * Retrieve the current entityManager
     */
    public static NoSqlEntityManager em() {
    	if (plugin2InClassPath) {
    		Class<?> clazz;
			try {
				clazz = Class.forName("com.alvazan.play2.NoSqlForPlay2");
				NoSqlInterface noSql2 = (NoSqlInterface) clazz.newInstance();
				return noSql2.em();
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("The play2 plugin class not found");
			} catch (InstantiationException e) {
				throw new RuntimeException("The play2 class can't be instantiated");
			} catch (IllegalAccessException e) {
				throw new RuntimeException("The play2 plugin class cannot be access");
			}
    	}
    	else
    		return get().entityManager;
    }

    /**
     * @return true if an entityManagerFactory has started
     */
    public static boolean isEnabled() {
        return entityManagerFactory != null;
    }

    /*
     * Build a new entityManager.
     * (In most case you want to use the local entityManager with em)
     */
    public static NoSqlEntityManager newEntityManager() {
        return entityManagerFactory.createEntityManager();
    }

	public static NoSqlEntityManagerFactory getEntityManagerFactory() {
		return entityManagerFactory;
	}

	static void setEntityManagerFactory(NoSqlEntityManagerFactory factory) {
		entityManagerFactory = factory;
	}

}
