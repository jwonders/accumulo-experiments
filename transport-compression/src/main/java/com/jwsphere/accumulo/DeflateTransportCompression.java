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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.thrift.ScanResult;
import org.apache.accumulo.core.data.thrift.TKey;
import org.apache.accumulo.core.data.thrift.TKeyValue;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

/**
 * Compresses the batch of KeyValue pairs by encoding a ScanResult thrift
 * object and deflating the resulting bytes, storing the compressed result
 * as the value for a singleton collection of TKeyValue objects.
 */
public class DeflateTransportCompression extends TransportCompression {

	@Override
	public List<TKeyValue> compress(List<? extends KeyValue> source) {
		ScanResult result = new ScanResult(toThrift(source), false);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DeflaterOutputStream deflateStream = new DeflaterOutputStream(baos);
		TTransport transport = new TIOStreamTransport(deflateStream);
		TCompactProtocol proto = new TCompactProtocol(transport);
		try {
			transport.open();
			result.write(proto);
		} catch (TException e) {
			throw new RuntimeException(e);
		} finally {
			transport.close();
		}
		byte[] bytes = baos.toByteArray();
		return Collections.singletonList(new TKeyValue(new TKey(), ByteBuffer.wrap(bytes)));
	}

	@Override
	public List<KeyValue> decompress(List<TKeyValue> data) {
		if (data.size() != 1) {
			throw new IllegalArgumentException("Unexpected number of results.");
		}
		byte[] bytes = data.get(0).getValue();
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		InflaterInputStream inflateStream = new InflaterInputStream(bais);
		TTransport transport = new TIOStreamTransport(inflateStream);
		TCompactProtocol proto = new TCompactProtocol(transport);
		ScanResult result = new ScanResult();
		try {
			transport.open();
			result.read(proto);
		} catch (TException e) {
			throw new RuntimeException(e);
		} finally {
			transport.close();
		}
		List<TKeyValue> decoded = result.getResults();
		return fromThrift(decoded);
	}

}
