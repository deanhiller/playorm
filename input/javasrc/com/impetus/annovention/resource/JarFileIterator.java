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
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.annovention.Filter;

/**
 * Iterates through a Jar file for each file resource.
 * 
 * @author animesh.kumar
 */
/**
 * @author animesh.kumar
 *
 */
public final class JarFileIterator implements ResourceIterator {

	private static final Logger log = LoggerFactory.getLogger(JarFileIterator.class);
	
    /** jar input stream */
    private JarInputStream jarInputStream;

    /** next entry */
    private JarEntry next;

    /** filter. */
    private Filter filter;

    /** initial. */
    private boolean start = true;

    /** closed. */
    private boolean closed = false;

    /**
     * Instantiates a new jar file iterator.
     * 
     * @param file
     * @param filter
     * @throws IOException
     */
    public JarFileIterator(File file, Filter filter) throws IOException {
        this(new FileInputStream(file), filter);
    }

    /**
     * Instantiates a new jar file iterator.
     * 
     * @param is
     * @param filter
     * @throws IOException
     */
    public JarFileIterator(InputStream is, Filter filter) throws IOException {
        this.filter = filter;
        jarInputStream = new JarInputStream(is);
    }

    // helper method to set the next InputStream
    private void setNext() {
    	start = true;
        try {
            if (next != null) {
            	jarInputStream.closeEntry();
            }
            next = null;

            do {
                next = jarInputStream.getNextJarEntry();
            } while (next != null && (next.isDirectory() || (filter == null || !filter.accepts(next.getName()))));

            if (next == null) {
                close();
            }
        } catch (IOException e) {
            throw new RuntimeException("failed to browse jar", e);
        }
    }

    /* @see com.impetus.annovention.resource.ResourceIterator#next() */
    @Override
    public InputStream next() {
        if (closed || (next == null && !start)) {
            return null;
        }
        setNext();
        if (next == null) {
            return null;
        }
        return new JarInputStreamWrapper(jarInputStream);
    }

    /* @see com.impetus.annovention.resource.ResourceIterator#close() */
    @Override
    public void close() {
        try {
            closed = true;
            jarInputStream.close();
        } catch (IOException ioe) {
        	log.warn("exception on close", ioe);
        }
    }

    /**
     * Wrapper class for jar stream
     */
    static class JarInputStreamWrapper extends InputStream {

        // input stream object which is wrapped
    	private InputStream is;

        public JarInputStreamWrapper(InputStream is) {
            this.is = is;
        }

        @Override
        public int read() throws IOException {
            return is.read();
        }

        @Override
        public int read(byte[] bytes) throws IOException {
            return is.read(bytes);
        }

        @Override
        public int read(byte[] bytes, int i, int i1) throws IOException {
            return is.read(bytes, i, i1);
        }

        @Override
        public long skip(long l) throws IOException {
            return is.skip(l);
        }

        @Override
        public int available() throws IOException {
            return is.available();
        }

        @Override
        public void close() throws IOException {
            // DO Nothing
        }

        @Override
        public void mark(int i) {
        	is.mark(i);
        }

        @Override
        public void reset() throws IOException {
            is.reset();
        }

        @Override
        public boolean markSupported() {
            return is.markSupported();
        }
    }
}
