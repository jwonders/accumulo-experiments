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
package com.jwsphere.accumulo.shell;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.interpret.ScanInterpreter;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.PrintFile;
import org.apache.accumulo.core.util.shell.commands.OptUtil;
import org.apache.accumulo.core.util.shell.commands.ScanCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.jwsphere.accumulo.DefaultTransportCompression;
import com.jwsphere.accumulo.DeflateTransportCompression;
import com.jwsphere.accumulo.DeflatedRelativeKeyTransportCompression;
import com.jwsphere.accumulo.RelativeKeyTransportCompression;
import com.jwsphere.accumulo.Scans;
import com.jwsphere.accumulo.TransportCompression;

/**
 * <p>
 * A command that evaluates the compression ratio achieved by several 
 * compression algorithms.  Currently supported algorithms include : <br>
 *   
 *   Default - delegates to {@code Key.compress} to replicate current functionality. <br>
 *   Deflated - deflates an encoded {@code ScanResult} thrift object. <br>
 *   RelativeKey - run-length encoding per-byte <br>
 *   DeflatedRelativeKey - deflates the run-length-encoded data <br>
 *   
 * </p>
 * 
 * <p>
 * Arguments are the same as the scan command with the single addition
 * of an argument to control the batch size.
 * </p>
 * 
 * <h3>
 * Usage Example:
 * </h3>
 * <p>
 * {@code TransportCompressionAnalyzer::scancompression -t my_table --batch-size 10000 }
 * <p>
 */
public class ScanCompressionCommand extends ScanCommand {

	private Option batchSizeOpt;

	@Override
	public Options getOptions() {
		batchSizeOpt = new Option(null, "batch-size", true, "scanner batch size");
		batchSizeOpt.setRequired(false);
		batchSizeOpt.setArgName("int");

		Options o = super.getOptions();
		o.addOption(batchSizeOpt);
		return o;
	}

	@Override
	public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws Exception {
		try {
			final PrintFile printFile = getOutputFile(cl);
			final String tableName = OptUtil.getTableOpt(cl, shellState);

			final ScanInterpreter interpeter = getInterpreter(cl, tableName, shellState);

			// handle first argument, if present, the authorizations list to
			// scan with
			final Authorizations auths = getAuths(cl, shellState);
			final Scanner scanner = shellState.getConnector().createScanner(tableName, auths);

			// handle session-specific scan iterators
			addScanIterators(shellState, cl, scanner, tableName);

			// handle remaining optional arguments
			scanner.setRange(getRange(cl, interpeter));

			// handle columns
			fetchColumns(cl, scanner, interpeter);

			// set timeout
			scanner.setTimeout(getTimeout(cl), TimeUnit.MILLISECONDS);
			scanner.setBatchSize(getBatchSize(cl));

			// output the records
			printRecords(cl, shellState, scanner, printFile);

			if (printFile != null) {
				printFile.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}

		return 0;
	}

	private int getBatchSize(CommandLine cl) {
		String batchSize = cl.getOptionValue(batchSizeOpt.getLongOpt());
		return Integer.parseInt(Optional.fromNullable(batchSize).or("1000"));
	}

	protected void printRecords(final CommandLine cl, Shell shellState, final Scanner scanner, PrintFile outFile) throws IOException {

		Iterator<List<KeyValue>> batched = Scans.toBatchIterator(scanner.iterator(), scanner.getBatchSize());

		final TransportCompression defaultCompression = new DefaultTransportCompression();
		final TransportCompression deflateCompression = new DeflateTransportCompression();
		final TransportCompression relativeKeyCompression = new RelativeKeyTransportCompression();
		final TransportCompression deflatedRelativeKeyCompression = new DeflatedRelativeKeyTransportCompression();

		Iterator<String> stats = Iterators.transform(batched, new Function<List<KeyValue>, String>() {
			public String apply(List<KeyValue> batch) {
				StringBuilder sb = new StringBuilder();
				sb.append("============ ScanResult compression stats =============").append('\n');
				sb.append(defaultCompression.evaluate(batch).toString()).append('\n');
				sb.append(deflateCompression.evaluate(batch).toString()).append('\n');
				sb.append(relativeKeyCompression.evaluate(batch).toString()).append('\n');
				sb.append(deflatedRelativeKeyCompression.evaluate(batch));
				sb.append("=======================================================").append("\n\n");
				return sb.toString();
			}
		});

		shellState.printLines(stats, true);
	}

	@Override
	public String description() {
		return "prints information about transport compression of scans";
	}

	@Override
	public int numArgs() {
		return 0;
	}

}
