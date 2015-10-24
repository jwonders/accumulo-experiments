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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.ScanResult;
import org.apache.accumulo.core.data.thrift.TKeyValue;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;


public abstract class TransportCompression {

	public abstract List<TKeyValue> compress(List<? extends KeyValue> data);
	public abstract List<KeyValue> decompress(List<TKeyValue> data);

	public CompressionStatistics evaluate(List<? extends KeyValue> data) {
		List<TKeyValue> uncompressed = toThrift(data);
		List<TKeyValue> compressed = compress(data);
		long compressedSize = computeSize(compressed);
		long uncompressedSize = computeSize(uncompressed);
		return new CompressionStatistics(this.getClass().getSimpleName(), uncompressedSize, compressedSize, data.size());
	}

	protected List<TKeyValue> toThrift(List<? extends KeyValue> data) {
		List<TKeyValue> results = new ArrayList<TKeyValue>();
		for (KeyValue kv : data) {
			results.add(new TKeyValue(kv.getKey().toThrift(), 
					ByteBuffer.wrap(kv.getValue().get(), 0, kv.getValue().getSize())));
		}
		return results;
	}
	
	protected List<KeyValue> fromThrift(List<? extends TKeyValue> data) {
		List<KeyValue> results = new ArrayList<KeyValue>();
		for (TKeyValue kv : data) {
			results.add(new KeyValue(new Key(kv.getKey()), new Value(kv.getValue())));
		}
		return results;
	}

	private long computeSize(List<TKeyValue> data) {
		ScanResult result = new ScanResult(data, false);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		TTransport transport = new TIOStreamTransport(baos);
		TCompactProtocol proto = new TCompactProtocol(transport);
		try {
			result.write(proto);
			transport.flush();
		} catch (TException e) {
			throw new RuntimeException(e);
		}
		return baos.toByteArray().length;
	}
}
