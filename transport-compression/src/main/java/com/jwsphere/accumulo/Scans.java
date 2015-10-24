package com.jwsphere.accumulo;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

public class Scans {
	
	private Scans() {
	}

	public static Iterator<List<KeyValue>> toBatchIterator(Iterator<Entry<Key, Value>> source, int batchSize) {
		return Iterators.partition(Iterators.transform(source, new Function<Entry<Key, Value>, KeyValue>() {
			public KeyValue apply(Entry<Key, Value> entry) {
				return new KeyValue(entry.getKey(), entry.getValue());
			}
		}), batchSize);
	}
	
	public static Iterable<List<KeyValue>> toBatchIterable(Iterable<Entry<Key, Value>> source, int batchSize) {
		return Iterables.partition(Iterables.transform(source, new Function<Entry<Key, Value>, KeyValue>() {
			public KeyValue apply(Entry<Key, Value> entry) {
				return new KeyValue(entry.getKey(), entry.getValue());
			}
		}), batchSize);
	}

}
