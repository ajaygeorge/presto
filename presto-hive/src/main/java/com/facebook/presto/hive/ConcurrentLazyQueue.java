/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import javax.annotation.concurrent.GuardedBy;

import java.util.Iterator;
import java.util.OptionalInt;

public class ConcurrentLazyQueue<E>
{
    @GuardedBy("this")
    private final Iterator<E> iterator;
    private volatile int size;

    public ConcurrentLazyQueue(Iterable<E> iterable, OptionalInt size)
    {
        this.iterator = iterable.iterator();
        this.size = size.orElse(-1);
    }

    public synchronized boolean isEmpty()
    {
        if (size >= 0) {
            return size == 0;
        }
        else {
            return !iterator.hasNext();
        }
    }

    public synchronized E poll()
    {
        if (iterator.hasNext()) {
            E next = iterator.next();
            if (size > 0) {
                size--;
            }
            return next;
        }
        return null;
    }
}
