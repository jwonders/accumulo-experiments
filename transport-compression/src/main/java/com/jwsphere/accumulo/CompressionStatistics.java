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

public class CompressionStatistics {

	private final String algorithm;
	private final long uncompressed;
	private final long compressed;
	private final int batchSize;
	
	public CompressionStatistics(String algorithm, long uncompressed, long compressed, int batchSize) {
		this.uncompressed = uncompressed;
		this.compressed = compressed;
		this.algorithm = algorithm;
		this.batchSize = batchSize;
	}
	
	/**
	 * A human-readable name of the compression algorithm.
	 */
	public String getAlgorithm() {
		return algorithm;
	}
	
	/**
	 * Number of key-value pairs in a batch.
	 */
	public int getBatchSize() {
		return batchSize;
	}
	
	/**
	 * Number of bytes after applying compression.
	 */
	public long getCompressedSize() {
		return compressed;
	}
	
	/**
	 * Number of bytes without applying compression.
	 */
	public long getUncompressedSize() {
		return uncompressed;
	}
	
	public double getCompressionRatio() {
		return (double) uncompressed / compressed;
	}
	
	public double getSavingsPercentage() {
		return 100 * (1.0 - (double) compressed / uncompressed);
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("algorithm : ").append(algorithm).append('\n');
		sb.append("batch size        : ").append(batchSize).append('\n');
		sb.append("uncompressed size : ").append(uncompressed).append('\n');
		sb.append("compressed size   : ").append(compressed).append('\n');
		sb.append("compression ratio : ").append(getCompressionRatio()).append('\n');
		sb.append("data savings pct  : ").append(getSavingsPercentage()).append('\n');
		sb.append("size per element  : ").append((double) compressed / batchSize).append('\n');
		return sb.toString();
	}
}
