package com.alvazan.orm.api.base.anno;

public enum FetchType {
	
	
	/**
	 * With OneToMany, List or Map, no entities are immediate fetched from the
	 * database.  As you loop through your list and do child.getId() the database
	 * is still not hit, BUT the first time as you loop through the children, you
	 * call child.get<ANY OTHER METHOD>(), there are two methods of loading from
	 * the database. FetchType.ONE_AT_A_TIME will ensure it fetches just that
	 * entity while FetchType.ALL_ONCE_HOT fetchs all entities as it assumes you
	 * are going to keep goin through the loop and calling child.getSomething() which
	 * would load each entity one at a time(which is a bad idea for performance if
	 * you have a lot of entities).
	 * 
	 * @return
	 */
	ALL_ONCE_HIT, 
	ONE_AT_A_TIME;
}
