This directory contains the protocol buffer definitions used by this project.

To build the Java classes based on these definition, run the following command in this directory:

protoc --java_out=../src/main/java/ reading_buffer.proto

Note: Assumes you have installed the protocol buffer compiler and it is in your path.

For more info, see: http://code.google.com/apis/protocolbuffers/
