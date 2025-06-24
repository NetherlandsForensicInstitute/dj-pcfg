#!/bin/bash
cd "$(dirname "$0")"
version=$(grep -oE '<version>[0-9]+\.[0-9]+\.[0-9]+' ../pom.xml | head -n 1 | cut -f2 -d">")

if [ ! -f ../target/dj-pcfg-$version.jar ];
then
	echo "Please build the project before running this script"
	echo "Run: mvn clean package -DskipTests"
	exit 1
fi

if [ -d pcfg_preprocessor-$version/ ];
then
	rm -rf pcfg_preprocessor-$version/
fi

if [ -d cache_server-$version/ ];
then
	rm -rf cache_server-$version/
fi

mkdir pcfg_preprocessor-$version
mkdir cache_server-$version

cp ../target/guesser-$version.jar pcfg_preprocessor-$version/guesser.jar
cp pcfg_wrapper.sh pcfg_preprocessor-$version/pcfg_preprocessor64.bin

cp ../target/cache-server-$version.jar cache_server-$version/

rm -f graalvm-jdk-23_linux-x64_bin.tar.gz*

wget https://download.oracle.com/graalvm/23/latest/graalvm-jdk-23_linux-x64_bin.tar.gz
mkdir jdk-23
tar -xzf graalvm-jdk-23_linux-x64_bin.tar.gz -C jdk-23 --strip-components=1
jdk-23/bin/jlink --output jre-23 --compress zip-9 --no-header-files --no-man-pages --module-path ../jmods --add-modules java.base,java.compiler,java.datatransfer,java.desktop,java.instrument,java.logging,java.management,java.management.rmi,java.naming,java.net.http,java.prefs,java.rmi,java.scripting,java.se,java.security.jgss,java.security.sasl,java.smartcardio,java.sql,java.sql.rowset,java.transaction.xa,java.xml,java.xml.crypto,jdk.graal.compiler
chmod -R +w jdk-23
cp -r jre-23 pcfg_preprocessor-$version
cp -r jre-23 cache_server-$version
rm -fr graalvm-jdk-23_linux-x64_bin.tar.gz jdk-23/ jre-23/
