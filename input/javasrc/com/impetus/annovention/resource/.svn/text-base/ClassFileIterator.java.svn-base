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
package com.impetus.annovention.resource;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.impetus.annovention.Filter;

/**
 * The Class ClassFileIterator.
 * 
 * @author animesh.kumar
 */
public class ClassFileIterator implements ResourceIterator {

    /** files. */
    private List<File> files;

    /** The index. */
    private int index = 0;

    /**
     * Instantiates a new class file iterator.
     * 
     * @param file
     * @param filter
     */
    public ClassFileIterator(File file, Filter filter) {
        files = new ArrayList<File>();
        try {
        	init(files, file, filter);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // helper method to initialize the iterator
    private static void init(List<File> list, File dir, Filter filter) throws Exception {
        File[] files = dir.listFiles();
        for (int i = 0; i < files.length; i++) {
            if (files[i].isDirectory()) {
            	init(list, files[i], filter);
            } else {
                if (filter == null || filter.accepts(files[i].getAbsolutePath())) {
                    list.add(files[i]);
                }
            }
        }
    }

    /* @see com.impetus.annovention.resource.ResourceIterator#next() */
    @Override
    public final InputStream next() {
        if (index >= files.size()){
            return null;
        }
        File fp = (File) files.get(index++);
        try {
            return new FileInputStream(fp);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /* @see com.impetus.annovention.resource.ResourceIterator#close() */
    @Override
    public void close() {
    	// DO Nothing
    }
}
