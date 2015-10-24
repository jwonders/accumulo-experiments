package com.jwsphere.accumulo;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;

public class TransportCompressionTest {

	
	public static void roundtrip(TransportCompression compressor) {
		List<KeyValue> entries = new ArrayList<KeyValue>();
		for (int i = 0; i < 20; ++i) {
			entries.add(generate());
		}
		List<KeyValue> decompressed = compressor.decompress(compressor.compress(entries));
		
		assertEquals(entries, decompressed);
	}
	
	public static KeyValue generate() {
		Key key = new Key(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
				UUID.randomUUID().toString(), "A&B&C", System.currentTimeMillis());
		Value value = new Value();
		return new KeyValue(key, value);
	}
	
}
