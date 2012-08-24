package com.alvazan.orm.util;

import java.util.Iterator;

public abstract class CachingIterator<E> implements Iterator<E> {

	private E cachedElement;
	
	@Override
	public boolean hasNext() {
		if(cachedElement != null)
			return true; //we already have loaded next element
		else if(hasNextImpl()) {
			//subclass tells us it has a subelement (and it caches that element so we can
			//call nextImpl and get it right now
			cachedElement = nextImpl();
			return true;
		}
		//we are fresh out of items...no more to process
		return false;
	}

	protected abstract boolean hasNextImpl();
	protected abstract E nextImpl();
	
	@Override
	public E next() {
		//If cachedElement is null, client may NEVER have called the hasNext method(this is allowed) so we need to 
		//check and call hasNextImpl just in case to load the next value but if there is no more items, we throw an exception
		if(cachedElement == null && hasNextImpl()) 
			throw new IllegalArgumentException("You ran out of items to iterate on, call iterator.hasNext instead to protect yourself from this nasty exception");
		E temp = cachedElement;
		cachedElement = null;
		return temp;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("not supported, probbably will never be..it's a bug if you hit this");
	}

}
