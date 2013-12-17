// Copyright 2013 Matthew Rathbone

//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at

//        http://www.apache.org/licenses/LICENSE-2.0

//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package com.matthewrathbone.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionCodec;
import java.io.BufferedWriter;
import java.io.File;
import java.io.StringWriter;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import org.apache.commons.io.IOUtils;

public class MRTester {
  private File base;
  private final Path baseDir;
  private JobConf conf = new JobConf();
  private FileSystem fileSystem;

  public static class JobArgs {
    public Configuration conf;
    public FileSystem fs;
    public JobArgs(Configuration conf, FileSystem fs) {
      this.conf = conf;
      this.fs = fs;
    }
  }

  public static abstract class TestJob {
    public abstract void run(JobArgs args) throws Exception;
  }

  public MRTester() throws Exception {
    this("target/test");
  }

  public MRTester(String b) throws Exception {
    this.base = new File(b);
    this.baseDir = new Path("file://" + base.getAbsolutePath());
    String mrDir = "mapred/local";
    String logs = "logs";
    
    conf.set("mapred.local.dir", new File(base, mrDir).getAbsolutePath());
    conf.set("mapred.job.tracker.handle.count", "2");
    conf.set("tasktracker.http.threads", "2");
    // avoid heap space issues during testing -- default is 100
    conf.set("io.sort.mb", "20");
    conf.set("mapred.map.max.attempts", "2");
    conf.set("mapred.reduce.max.attempts", "2");
    conf.set("jobclient.completion.poll.interval", "30");
    System.getProperties().setProperty("hadoop.log.dir", new File(base, logs).getAbsolutePath());
    this.fileSystem = FileSystem.get(baseDir.toUri(), conf);
    assert(fileSystem.mkdirs(new Path(baseDir, mrDir)));
    assert(fileSystem.mkdirs(new Path(baseDir, logs)));
  }

  public Path registerDirectory(boolean create) throws Exception {
    String dirName = UUID.randomUUID().toString();
    Path destination = new Path(baseDir, dirName);
    fileSystem.delete(destination, true);
    if (create) fileSystem.mkdirs(destination);
    return destination;
  }

  public Path registerDirectory() throws Exception {
    return registerDirectory(true);
  }

  public Path registerString(String value) throws Exception {
    Path dir = registerDirectory();
    Path file = new Path(dir, "file");
    BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fileSystem.create(file,true)));
    try {
      br.write(value);  
    } finally {
      br.close();
    }
    return dir;
  }

  public List<String> collectStrings(Path location) throws Exception {
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    FileStatus[] items = fileSystem.listStatus(location);
    if (items == null) return new ArrayList<String>();
    List<String> results = new ArrayList<String>();
    for(FileStatus item: items) {
      if(item.getPath().getName().startsWith("_")) {
        continue;
      }

      CompressionCodec codec = factory.getCodec(item.getPath());
      InputStream stream = null;

      // check if we have a compression codec
      if (codec != null) {
        stream = codec.createInputStream(fileSystem.open(item.getPath()));
      }
      else {
        stream = fileSystem.open(item.getPath());
      }

      StringWriter writer = new StringWriter();
      IOUtils.copy(stream, writer, "UTF-8");
      String raw = writer.toString();
      String[] resulting = raw.split("\n");
      for(String str: raw.split("\n")) {
        results.add(str);
      }
    }
    return results;
  }

  public void run(TestJob job) throws Exception {
    JobArgs args = new JobArgs(conf, fileSystem);
    job.run(args);
  }
}
