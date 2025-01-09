#!/bin/bash

wget https://download.oracle.com/graalvm/23/latest/graalvm-jdk-23_linux-x64_bin.tar.gz
mkdir jdk-23
tar -xzf graalvm-jdk-23_linux-x64_bin.tar.gz -C jdk-23 --strip-components=1
jdk-23/bin/jlink --output jre-23 --compress zip-9 --no-header-files --no-man-pages --module-path ../jmods --add-modules java.base,java.compiler,java.datatransfer,java.desktop,java.instrument,java.logging,java.management,java.management.rmi,java.naming,java.net.http,java.prefs,java.rmi,java.scripting,java.se,java.security.jgss,java.security.sasl,java.smartcardio,java.sql,java.sql.rowset,java.transaction.xa,java.xml,java.xml.crypto,jdk.graal.compiler
mv jre-23 pcfg_preprocessor-*
chmod -R +w jdk-23
rm -r graalvm-jdk-23_linux-x64_bin.tar.gz jdk-23