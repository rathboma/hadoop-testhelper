# A Simple Test Runner for Hadoop Tasks in Java and Scala

Test your Hadoop projects easily and quickly.

The library takes care of setting up local hadoop configuration so your code runs in local mode. Unlike MRUnit, this is more of an end-to-end test framework, so you can test your entire pipeline quickly and easily.
## Notes on Completeness

This is a very early (but working!) version of the library, please provide feedback and send pull requests and I will do my best to accomodate you.


## Java Example

```java
import com.matthewrathbone.hadoop.MRTester;
import com.matthewrathbone.hadoop.MRTester.*;

private String testInput = "1,2,3\n1,2,3\n1,2,3";

public void testMyHadoopJob() {
  MRTester tester = new MRTester();
  TestJob job = new TestJob() {
    @Override
    public void run(JobArgs args) throws Exception {

      // the tester can provide you with some directories to use.
      Path inputDirectory = tester.registerString(testInput);
      Path outputDirectory = tester.registerDirectory();
      
      // run your mapreduce pipeline
      // I always add a second main method that takes a Configuration object to override for testing.
      // the regular main just calls that with a new Configuration()
      MyMapReducePipeline.main(inputDirectory.toString(), outputDirectory.toString(), args.conf);

      // these are the outputs to our job, we can now assert results. It can read compressed files.
      List<String> results = runner.collectStrings(outputDirectory);
      Assert.assertEquals("number of results is correct", results.size(), 10);

      // we also have access to the filesystem if we want to check other stuff:
      args.fs.listStatus(...);
    }
  }

  // code gets executed here
  tester.run(job);
}

```

## Scala Example

I've added a small convenience wrapper for the scala version, so it's even easier. Here is the same example but in Scala:

I suggest using scalaj-collection to convert between java and scala types: https://github.com/scalaj/scalaj-collection


```Scala
import com.matthewrathbone.ScalaMRTester

val input = List("1,2,3", "1,2,3", "1,2,3").mkString("\n")

def testMyHadoopJob() {
  val tester = new ScalaMRTester()
  tester.run{ args =>
    val inputDirectory = tester.registerString(input)
    val outputDirectory = tester.registerDirectory()
    MyMapReducePipeline.main(inputDirectory, outputDirectory, args.conf)
    val results = runner.collectStrings(outputDirectory).asScala // using scalaj-collection
    results.foreach{ result =>
      Assert.assertEquals(result, "something")
    }
  }
}
```


## Inspiration

The code is based on a test library I built when working at [Foursquare](http://foursquare.com), it's almost identical in structure, although a lot less complete. It proved very reliable and we ran many hundereds of tests using it.

I missed it so much that I created the code you see here to use in my [Hadoop Examples](https://github.com/rathboma/hadoop-framework-examples) project, hopefully someone else finds it as useful as I have.


## TODO (\*cough\* for pull requests \*cough\*)

- helper methods for reading / writing sequence files
- helper methods for collecting other object types from the output files (writables)
- validating whether tests using this library can be run reliably in parallel (I think so)