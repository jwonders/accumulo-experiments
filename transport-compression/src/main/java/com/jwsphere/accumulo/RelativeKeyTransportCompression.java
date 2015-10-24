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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.TKey;
import org.apache.accumulo.core.data.thrift.TKeyValue;
import org.apache.accumulo.core.file.rfile.RelativeKey;

/**
 * Compresses the data by computing relative keys for sequential keys in order to 
 * perform run-length encoding per byte.  All RLE compressed pairs are written to 
 * a single buffer which is stored as the value in a singleton TKeyValue collection. 
 */
public class RelativeKeyTransportCompression extends TransportCompression {

	public List<TKeyValue> compress(List<? extends KeyValue> source) {
		Key prev = new Key();
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(baos);
			out.writeInt(source.size());
			for (KeyValue kv : source) {
				RelativeKey rk = new RelativeKey(prev, kv.getKey());
				try {
					rk.write(out);
					out.writeInt(kv.getValue().getSize());
					out.write(kv.getValue().get(), 0, kv.getValue().getSize());
				} catch (IOException e) { 
					throw new RuntimeException(e);
				}
				prev = kv.getKey();
			}
			out.flush();
			return Collections.singletonList(new TKeyValue(new TKey(), ByteBuffer.wrap(baos.toByteArray())));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<KeyValue> decompress(List<TKeyValue> data) {
		if (data.size() != 1) {
			throw new IllegalArgumentException("Unexpected number of results.");
		}
		byte[] bytes = data.get(0).getValue();
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		DataInputStream in = new DataInputStream(bais);
		try {
			int size = in.readInt();
			List<KeyValue> decoded = new ArrayList<KeyValue>();
			Key prev = new Key();
			for (int i = 0; i < size; ++i) {
				RelativeKey rk = new RelativeKey();
				rk.setPrevKey(prev);
				rk.readFields(in);
				int valueSize = in.readInt();
				byte[] valueBytes = new byte[valueSize];
				in.read(valueBytes);
				Value value = new Value(valueBytes);
				decoded.add(new KeyValue(rk.getKey(), value));
				prev = rk.getKey();
			}
			return decoded;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
