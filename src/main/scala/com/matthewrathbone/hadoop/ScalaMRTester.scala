
package com.matthewrathbone.hadoop

import com.matthewrathbone.hadoop.MRTester._

class ScalaMRTester extends MRTester {

  def run(func: (JobArgs) => Unit) {
    run(new TestJob() {
      override def run(args: JobArgs) = func(args)
    })
  }
}