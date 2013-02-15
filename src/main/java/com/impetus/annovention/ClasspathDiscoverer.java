/*
 * Copyright 2010 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.annovention;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class ClasspathReader.
 * 
 * @author animesh.kumar
 */
public class ClasspathDiscoverer extends Discoverer {

	private static final Logger log = LoggerFactory.getLogger(ClasspathDiscoverer.class);
	
	/** The filter. */
	private Filter filter;

	/**
	 * Instantiates a new classpath reader.
	 */
	public ClasspathDiscoverer() {
		filter = new FilterImpl();
	}

	/**
	 * Uses java.class.path system-property to fetch URLs
	 * 
	 * @return the URL[]
	 */
	@Override
	public final URL[] findResources(ClassLoader cl) {
		List<URL> list = new ArrayList<URL>();
		
		Enumeration<URL> resources = loadPersistenceFiles(cl);
		
		log.trace("about to log the jar resources here that contain nosql.Persistence.class...");
		Set<URL> urlsToScan = new HashSet<URL>();
		while(resources.hasMoreElements()) {
			URL url = resources.nextElement();
			if(urlsToScan.contains(url))
				continue;
			
			urlsToScan.add(url);
			log.trace("found url with nosql.Persistence.class so scanning enabled on this url="+url);
		}
		
		if(urlsToScan.size() == 0)
			throw new IllegalArgumentException("We did not fine the nosql.Persistence.class file on the classpath for any jar.  This file needs to exist in the jars with @Entity objects and we will only scan those jars(or folders)");
		
		//At this point, we have urls that point to NoSqlPersistence.class which may be in a jar or in 
		//a folder.
		for(URL fileInJarOrFolderUrl : urlsToScan) {
			String protocol = fileInJarOrFolderUrl.getProtocol();
			if("file".equals(protocol)) {
				processFile(fileInJarOrFolderUrl, list);
			} else if("jar".equals(protocol)) {
				processJar(fileInJarOrFolderUrl, list);
			} else 
				throw new RuntimeException("protocol of="+protocol+" is not supported for loading classfiles, let me know and I can fix that");
		}

		
		return list.toArray(new URL[list.size()]);
	}

	private void processJar(URL fileInJarOrFolderUrl, List<URL> list) {
		String file = fileInJarOrFolderUrl.getFile();
		
		//format of jar file is file:/Users/dhiller2/AAROOT/area1/fullSDI/restApi/lib/nosqlorm.jar!/nosql/Persistence.class
		//so we split on ! first nd get the first piece.
		String[] pieces = file.split("!");
		String firstPiece = pieces[0];
		
		//Now we have firstPiece = file:/Users/dhiller2/AAROOT/area1/fullSDI/restApi/lib/nosqlorm.jar
		String prefix = "jar:"+firstPiece+"!/";
		URL url = createUrl(prefix);
		log.info("adding jar file for scanning="+url);
		list.add(url);
	}

	private URL createUrl(String prefix) {
		try {
			URL url = new URL(prefix);
			return url;
		} catch(MalformedURLException e) {
			throw new RuntimeException(e);
		}
	}

	private void processFile(URL fileInJarOrFolderUrl, List<URL> list) {
		String file = fileInJarOrFolderUrl.getFile();
		File f = new File(file);
		File classFolder = f.getParentFile().getParentFile();

		URL url = toUrl(classFolder);
		log.info("adding folder to scan="+url);
		list.add(url);
	}

	private URL toUrl(File f) {
		try {
			return f.toURI().toURL();
		} catch (MalformedURLException e) {
			throw new RuntimeException("bug in converting file to url="+f.getAbsolutePath(), e);
		}
	}

	private Enumeration<URL> loadPersistenceFiles(ClassLoader cl) {
		try {
			Enumeration<URL> resources = cl.getResources("nosql/Persistence.class");
			return resources;
		} catch(IOException e) {
			throw new RuntimeException("some kind of bug", e);
		}
		
	}

	/* @see com.impetus.annovention.Discoverer#getFilter() */
	public final Filter getFilter() {
		return filter;
	}

	/**
	 * @param filter
	 */
	public final void setFilter(Filter filter) {
		this.filter = filter;
	}
}
