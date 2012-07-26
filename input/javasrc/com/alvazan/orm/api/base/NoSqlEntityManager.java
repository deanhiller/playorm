package com.alvazan.orm.api.base;

import java.util.List;

import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.layer1.base.BaseEntityManagerImpl;
import com.google.inject.ImplementedBy;

@ImplementedBy(BaseEntityManagerImpl.class)
public interface NoSqlEntityManager {

	public static final String META_DB_KEY = "nosqlorm";
	
	/**
	 * Retrieve underlying interface to write raw columns to.  This works the same as the NoSqlEntityManager
	 * in that you must call flush to execute all the calls to persist.
	 * 
	 * @return 
	 */
	public NoSqlSession getSession();

	public void put(Object entity);
	
	public void putAll(List<Object> entities);
	
	public <T> T find(Class<T> entityType, Object key);
	
	public <T> List<KeyValue<T>> findAll(Class<T> entityType, List<? extends Object> keys);
	
	public <T> T getReference(Class<T> entityType, Object key);
	
	/**
	 * Unlike RDBMS, there are no transactions, BUT all the calls to putAll and put are cached
	 * in-memory until flush is called.  This allows us to easily queue up all writes to
	 * the datastore and 
	 * <ol>
	 *    <ul> Send all writes as one to incur less i/o over the network
	 *    <ul> not block your thread for every put/putAll that is called(only block on writing all during flush)
	 *    <ul> time how long all the writes take.  
	 * </ol>
	 * 
	 * It is more likely to keep things more consistent
	 */
	public void flush();
	
	/**
	 * Not ready for use as of yet.  
	 * 
	 * Best explained with an example.  Let's say you have a table with 1 billion rows
	 * and let's say you have 1 million customers each with on avera 1000 rows in that
	 * table(ie. total 1 billion).  In that case, it would be good to create 1 million
	 * indexes with 1000 nodes in them each rather than one 1 billion node index as it
	 * would be 1000's of times faster to fetch the small index and query it.  Pretend
	 * the entity representing this 1 billion row table was ActionsTaken.class, then to
	 * get the index for all rows relating to a user that you can then query you would
	 * call entityMgr.getIndex(ActionsTaken.class, "/byUser/"+user.getId());
	 * 
	 * Going on, let's say that same 1 billion activity table is also related to 500k
	 * commentors.  You may have another 500k indexes for querying when you have the 
	 * one commentor and want to search on other stuff so you would get the index to
	 * query like so entityMgr.getIndex(ActionsTaken.class, "/byCommentor/"+commentor.getId());
	 * 
	 * In this methodology you are just breaking up HUGE tables into small subtables that
	 * can be queried.
	 * 
	 * @param forEntity
	 * @param indexName
	 * @return
	 */
	public <T> Index<T> getIndex(Class<T> forEntity, String indexName);

	/**
	 * In certain cases where you have a bi-directional association, you need a primary key in
	 * the children before you can save the parent.  ie. If you have an Account that has a list
	 * of Activity and an Activity with an Account, the proper way to code this is such
	 * 
	 * <ol>
	 *    <li>Account acc = new Account() </li>
	 *    <li>entityManager.fillInWithKey(acc) </li>
	 *    <li>Activity act1 = new Activity() </li>
	 *    <li>act1.setAccount(acc) </li>
	 *    <li>entityManager.put(act1) //act1 key is generated and filled in here</li>
	 *    <li>acc.addActivity(act1) //here account has an act1 WITH a key</li>
	 *    <li>entityManager.put(acc) </li>
	 *  </ol>
	 *  
	 * @param acc
	 */
	public void fillInWithKey(Object acc);
	
	/**
	 * Mainly for framework code but a nice way to get the key of an unknown entity
	 * where you don't care about the entity but just need the key
	 * @param entity
	 * @return
	 */
	public Object getKey(Object entity);

	/**
	 * This is a convenience method for in memory database and in-memory index so that
	 * unit tests can create the whole entityManagerFactory ONCE in @BeforeClass and then
	 * in @After, they can easily clear all state from the test with this method.
	 * 
	 * This method ONLY works with the DbTypeEnum.IN_MEMORY
	 */
	public void clearDbAndIndexesIfInMemoryType();
}
