/*
 Copyright (c) 2015 by ScaleOut Software, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package com.scaleoutsoftware.soss.hserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

/**
 * This factory encapsulates the input and output stream, which can be used on both sides of
 * the connection to compress the flowing data and reduce its size.
 * The streams cache and reuse {@link Inflater} and {@link Deflater} objects, so the
 * stream of this objects does not stress the garbage collector. {@link Inflater} and {@link Deflater}
 * are know to take significant time to collect because they implement non-trivial
 * finalize() methods (http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4797189).
 */
public class CompressingStreamsFactory {
    static abstract class ResourceCache<RESOURCE> {
        private Queue<RESOURCE> cache = new LinkedBlockingQueue<RESOURCE>();

        public RESOURCE get() {
            RESOURCE resource = cache.poll();
            return resource != null ? resource : newResource();
        }

        public void returnResource(RESOURCE resource) {
            cache.offer(resource);
        }

        public abstract RESOURCE newResource();
    }

    static class CompressedOutputStream extends DeflaterOutputStream {
        private static ResourceCache<Deflater> deflaterCache = new ResourceCache<Deflater>() {
            @Override
            public Deflater newResource() {
                return new Deflater(Deflater.DEFAULT_COMPRESSION, true);
            }
        };


        public CompressedOutputStream(OutputStream out) {
            super(out, deflaterCache.get(), 512);
            //usesDefaultDeflater is false (default value);
        }

        /**
         * Finishes writing compressed data to the output stream without closing
         * the underlying stream. Use this method when applying multiple filters
         * in succession to the same output stream.
         *
         * @throws IOException if an I/O error has occurred
         */
        public void finish() throws IOException {
            if (!def.finished()) {
                def.finish();
                while (!def.finished()) {
                    int len = def.deflate(buf, 0, buf.length);
                    out.write(buf, 0, len);
                }
            }
        }

        @Override
        public void close() throws IOException {
            //Do not close the underlying stream
            finish();
            def.reset();
            deflaterCache.returnResource(def);
        }
    }

    static class CompressedInputStream extends InflaterInputStream {
        private static ResourceCache<Inflater> inflaterCache = new ResourceCache<Inflater>() {
            @Override
            public Inflater newResource() {
                return new Inflater(true);
            }
        };

        public CompressedInputStream(InputStream in) {
            super(in, inflaterCache.get(), 512);
        }

        @Override
        public void close() throws IOException {
            //Do not close the underlying stream
            inf.reset();
            inflaterCache.returnResource(inf);
        }
    }

    /**
     * Gets the compressed output stream, wrapping the provided output stream.
     *
     * @param os output stream to wrap
     * @return compressed output stream
     */
    public static OutputStream getCompressedOutputStream(OutputStream os) {
        return new CompressedOutputStream(os);
    }

    /**
     * Gets the compressed input stream, wrapping the provided input stream.
     *
     * @param is input stream to wrap
     * @return compressed wrapping input stream
     */
    public static InputStream getCompressedInputStream(InputStream is) {
        return new CompressedInputStream(is);
    }
}
