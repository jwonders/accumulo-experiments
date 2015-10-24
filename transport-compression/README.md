# transport-compression

This project contains an Accumulo Shell Extension that computes the transport
compression ratio achieved with several different algorithms given a sequential
scan.

## Building

    mvn clean package
    
## Installation

Place the jar artifact created from the build process on the Accumulo classpath,
typically either the `accumulo/lib` or `accumulo/lib/ext` paths in order to get
the shell to pick up the extension.  Depending on the classpath configuration
this may require a restart.

## Usage

The command takes the same arguments as the `scan` command with one extra optional
argument to control the batch size (which can have a big effect on compression).

    ./bin/accumulo shell -u user -p pass
    > extensions --enable
    > TransportCompressionAnalyzer::scancompression -t my_table --batch-size 10000
    
