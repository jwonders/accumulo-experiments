/**
 * Copyright 2015 Jonathan Wonders
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jwsphere.accumulo;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.TKeyValue;

/**
 * Applies the default compression strategy which will omit the row, column family,
 * column qualifier, or column visibility if the previous key has an equivalent 
 * value for the respective key component.  Timestamps are not compressed.
 */
public class DefaultTransportCompression extends TransportCompression {

	@Override
	public List<TKeyValue> compress(List<? extends KeyValue> source) {
		return Key.compress(source);
	}

	@Override
	public List<KeyValue> decompress(List<TKeyValue> data) {
		Key.decompress(data);
		List<KeyValue> decompressed = new ArrayList<KeyValue>();
		for (TKeyValue tkv : data) {
			decompressed.add(new KeyValue(new Key(tkv.getKey()), new Value(tkv.getValue())));
		}
		return decompressed;
	}

}
