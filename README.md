About
-
Java implementation of the [PCFG password guesser](https://github.com/lakiw/pcfg_cracker).

Only implements the guessing part. Training should still be done with the python trainer,
the resulting model can be used by this implementation. This implementation can also be used to run a PCFG attack in a distributed way on [Hashtopolis](https://github.com/hashtopolis/server).
It can also be executed locally using multiple threads for increased performance.

It implements a subset of the python guesser implementation. A not included (yet) shortlist:
* honeywords is not supported
* does not implement OMEN (so no brute-force)
* case mangling is always enabled

A 'new' features shortlist:
* ability to skip the first *n* passwords
* ability to show the keyspace of a given rule
* ability to share progress between running instances in order to improve performance
* multithreading (but can't run massively parallel)

Apart from the guesser it also implements a serializer for serializing a trained grammer to a binary file. This improves
the performance of loading a model and makes it easier to upload it to Hashtopolis.

**Note** about running a Hashtopolis attack: when running an attack, Hashtopolis will 'split' the attack into chunks, which will be distributed
between the nodes. For a given chunk it will use the `skip` and `limit` commands to instruct the guesser to generate
only the passwords in that range. In order for the guesser not having to skip all previous passwords each time
it receives a new chunk, it can cache the end state of a run after finishing a chunk. Next time, it can load this
state and use it as a starting point, which will most likely be closer to the start of the new chunk. Ideally,
this state should be shared between nodes, so that new nodes which get assigned to a task in a later point in time,
can also make use of this cached information. Currently, this is possible if the nodes have access to a shared filesystem, see later on.

Requirements
-
Requirements for executing:
* JRE >= 23 (Java runtime)

Requirements for development:
* JDK >= 23 (Java development kit)
* Maven (tested with 3.6.3)

Build
-
Building executable jars for the guesser and grammar serializer:
```shell
mvn clean package -DskipTests
```

Will result in various runnable jars, the most important being:
```
target/guesser-{version}.jar
target/serializer-{version}.jar
target/cache-server-{version}.jar
```

Run
-
Executing the guesser:
```
java -jar target/guesser-{version}.jar
Missing required option: '--rule=<rulePath>'
Usage: pcfg_guesser [--keyspace] [--cache_directory_path=<cachePath>]
                    [--cache_server_address=<cacheServerAddress>]
                    [--limit=<limit>] [--log_directory_path=<logPath>]
                    [--max_keyspace=<maxKeyspace>] [--mode=<mode>]
                    [--output=<outputPath>]
                    [--producer_thread_count=<threadCount>] --rule=<rulePath>
                    [--skip=<skip>]
      --cache_directory_path=<cachePath>
                          Load/store state using given cache directory while
                            generating guesses
      --cache_server_address=<cacheServerAddress>
                          Load/store state using given caching server
      --keyspace          Return the number of passwords that can be generated
                            from the model
      --limit=<limit>     Output at most <limit> passwords
      --log_directory_path=<logPath>
                          Directory where to store live and archived log files
      --max_keyspace=<maxKeyspace>
                          Limit the value returned by the keyspace to this
                            number
      --mode=<mode>       Valid values: TRUE_PROB_ORDER, RANDOM_WALK (case
                            insensitive)
      --output=<outputPath>
                          The file to write the guesses to
      --producer_thread_count=<threadCount>
                          Use <count> threads for generation (experimental)
      --rule=<rulePath>   The PCFG rule to start generating guesses from
      --skip=<skip>       Skip the first <skip> passwords before starting to
                            output
```
Executing the serializer:
```
java -jar target/serializer-{version}.jar 
Missing required option: '--input=<inputPath>'
Usage: pcfg_serializer --input=<inputPath> [--output=<outputPath>]
      --input=<inputPath>   The directory of the PCFG rule to serialize
      --output=<outputPath> The file to write the serialized binary rule to
                              (defaults to [inputPath].pbm)
```
Executing the cache server:
```
java -jar target/cache-server-{version}.jar 
Missing required options: '--port=<port>', '--cache_directory_path=<cachePath>'
Usage: pcfg_cache_server --cache_directory_path=<cachePath>
                         [--log_directory_path=<logPath>] --port=<port>
                         [--rpc_thread_count=<threadCount>]
      --cache_directory_path=<cachePath>
                      Load/store state in given cache directory
      --log_directory_path=<logPath>
                      Directory where to store live and archived log files
      --port=<port>   Port to start listening on
      --rpc_thread_count=<threadCount>
                      Use <count> threads for processing client requests
```
Test
-
Testing with maven:
```shell
mvn clean test
```

Use on Hashtopolis
-
Using it on Hashtopolis requires two things: adding a guesser instance as a preprocessor, and
uploading a ruleset to crack with. When running an attack, some care must be taken about which
options to use. See below for more information.

**Build and add the preprocesor:**

Will be automated at some point. For now, some manual actions have to be taken when you want to deploy a
custom guesser. 

Note that this example packages a JRE within the preprocessor. You could also install it systemwide on
each agent node, or just put the JRE directory somewhere on the filesystem. In both cases you should update
the `deploy/pcfg_preprocessor-{version}/pcfg_preprocessor64.bin` shell script to refer to the `java` executable.

Create new guesser:
```shell
mvn clean package -DskipTests
mv target/guesser-{version}.jar deploy/pcfg_preprocessor-{version}/guesser.jar
```
Test if it works locally:
```shell
cd deploy
# if not yet present, download and build a jre, e.g. using the following script
# ./build_jre.sh
pcfg_preprocessor-{version}/pcfg_preprocessor64.bin pcfg_preprocessor-{version}/guesser.jar
```
Build preprocessor:
```shell
7z a pcfg_preprocessor-{version}.7z pcfg_preprocessor-{version}
```
Make it accessible to Hashtopolis, e.g. by uploading it to the binaries directory:
```shell
scp pcfg_preprocessor-{version}.7z {hashtopolis}:/opt/hashtopolis/binaries
```
Finally, add a new preprocessor in Hashtopolis by going to *Config > Preprocessors > Add* and entering:
* *Name*: can be anything, e.g. `pcfg {version}`
* *Binary Basename*: `./pcfg_preprocessor`
* *Download URL*: location of the binary, e.g. `{hashtopolis}/binaries/pcfg_preprocessor-{version}.7z`
* *Commands*:
  * `--keyspace`
  * `--skip`
  * `--limit`

**Add PCFG rule:**

The best way is to use the serializer to generate a binary version of the trained rule, before uploading
it to Hashtopolis.

Create serializer:
```shell
mvn clean package -DskipTests
```
Serialize a rule:
```shell
java -jar target/serializer-{version}.jar --input {rule_directory_path} --output {output_file_path}
```
This file can be uploaded to Hashtopolis using the standard means.

**Running a true probability order attack:**

Start by creating a new task using *Tasks > New Task*. Add the uploaded binary rule
as a preprocessor file by checking the checkbox in the P column in front of it.

Next, choose `Yes` in *Set as preprocessor task*. The rule name will already be filled in the textbox
below the dropdown menu. Change it to something like:
```shell
--rule {rule_file_name} --max_keyspace 1000000 --log_directory_path {some_directory}/logs --max_heap 32G --producer_thread_count 4
```
Explanation:
* `--rule`: the path of the rule file [**required**]
* `--max_keyspace`: makes Hashtopolis think given rule contains at most *n* passwords. Useful for limiting the amount of passwords to attack, 
and to allow Hashtopolis progress reporting to work when the real keyspace would be too large (which is often the case). Will probably be the
same number you would pass to *limit* when using python PCFG
* `--log_directory_path`: directory where the guesser will store its log files (can be used by multiple nodes if put on a shared filesystem)
* `--max_heap`: max heap memory used by the Java runtime (same as the Xmx parameter passed to the java process)
* `--producer_thread_count`: number of password generating threads to use (not total amount of process threads)

**!! Note** that this setup does not keep track of earlier states, or shares state between Hashtopolis nodes. This means each chunk will make the guesser start skipping from keyspace position 0.
In order to make use of state sharing (highly recommended), there are two mutually exclusive options to use:
* `--cache_directory_path`: store and load state from given directory. State can be shared between different nodes if the directory path points to
a shared filesystem directory (e.g. NFS share, only tested with this). Using it in combination with such a share is not entirely robust,
but good enough for basic attacks

An example preprocessor command would be:
```
--rule {rule_file_name} --max_keyspace 1000000 --cache_directory_path {some_directory}/checkpoint_cache --log_directory_path {some_directory}/logs --max_heap 32G --producer_thread_count 4
```
* `--cache_server_address`: load and store state from using a cache server. This requires a running cache server before starting the attack, for example
on the same node as the Hashtopolis instance [**experimental!**]

Setting up the cache server requires you to run the `cache-server-{version}.jar` jar. Example shell command:
```shell
java -jar cache-server-{version}.jar --port 60000 --cache_directory_path {some_directory}/checkpoint_cache --log_directory_path {some_directory}/logs 
```
An example preprocessor command would be:
```
--rule {rule_file_name} --max_keyspace 1000000 --cache_server_address {cache_server_host}:6000 --log_directory_path {some_directory}/logs --max_heap 32G --producer_thread_count 4
```

Finally, the current Hashtopolis agent can't benchmark a preprocessor task correctly, so static chunking is required.
So for *Use static chunking*, pick `Fixed chunk size`. Fill in a size which will make the task run for an expected amount of time.
Note that the guesser will not run exactly the same speed over the course of the keyspace. Most likely it will slow down.

It may be necessary to extend the Hashcat timeout abort in case a rule takes a while. This can be
configured by adding e.g. `--stdin-timeout-abort=36000` to *Attack command*, which increases the time
to 10 hours (from the default 5 minutes). It can also be disabled instead by setting it to `0`.

## Important

1) Try to configure chunk sizes which are not too small (e.g. 1 hour), especially when you share the cached state between checkpoints.
2) The `--max_heap` also determines the possible size of the output cache state. If this state would be shared between nodes, you should limit the heap size to that of the node with the lowest amount of resources.

TODO
-
- [ ] add some code documentation
- [ ] allow sharing checkpoints between Hashtopolis nodes (in absence of shared FS)
- [ ] improve checkpointing (atomicity, more even distribution, size handling)
- [ ] add missing features of python implementation, where possible
- [ ] take a look at libraries for common functionality (e.g. serialization, instead of the current DIY)
- [ ] ...