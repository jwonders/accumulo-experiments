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

import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellExtension;

/**
 * A shell extension that supplies commands for evaluating various
 * transport compression algorithms for sending data from tablet
 * servers to clients during scans (primarily sequential scans). 
 */
public class ScanCompressionAnalyzerShellExtension extends ShellExtension {

	@Override
	public String getExtensionName() {
		return "TransportCompressionAnalyzer";
	}

	@Override
	public Command[] getCommands() {
		return new Command[] { new ScanCompressionCommand() };
	}

}
