package com.alvazan.orm.api;

import java.util.List;

public interface NoSqlEntityManager {

	/**
	 * Some programmers I have met think random == unique.  It does NOT.  Generate 11 random
	 * numbers between 1 and 10 and the 11th "randomly generated number will match one of the
	 * previous 10 meaning it is NOT unique.  That said, we could have used
	 * http://johannburkard.de/blog/programming/java/Java-UUID-generators-compared.html
	 * to generate UUID's here but instead we wanted smaller keys so we use the following method
	 * for generation which WILL be unique within a cluster of machines with unique ips.  The
	 * algorithm is VERY simple host-specialtimestamp where special timestamp is time in 
	 * millis BUT in the case where two people called this method on the same machine, the last
	 * timestamp given was stored so we increment by one such that there will never be 
	 * a non-unique key.  
	 * 
	 * @return
	 */
	//public String generateUniqueKey();
	
	public void put(Object entity);
	
	public void putAll(List<Object> entities);
	
	public <T> T find(Class<T> entityType, Object key);
	
	public <T> List<T> findAll(Class<T> entityType, List<Object> keys);
	
	public <T> T getReference(Class<T> entityType, Object key);
	
	/**
	 * Unlike RDBMS, there are no transactions, BUT all the calls to putAll are cached
	 * in-memory into flush is called.  This allows us to easily queue up all writes to
	 * the datastore and time how long all the writes take.  It is also a bit more likely
	 * to keep things more consistent
	 */
	public void flush();
	
	/**
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
}
