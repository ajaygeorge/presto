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
package com.facebook.presto.server;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;

public class LongSetSerializer
        extends JsonSerializer<LongSet>
{
    @Override
    public void serialize(LongSet longSet, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException
    {
        long[] longs = longSet.toLongArray();
        jsonGenerator.writeArray(longs, 0, longs.length);
    }
}