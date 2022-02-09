package com.circle.data.glue

import com.amazonaws.services.glue.DynamicFrame
import org.apache.spark.SparkException

trait JobsErrorChecker {

  def checkForErrors(dynamicFrame: DynamicFrame, jobName: String, showAll: Boolean = true) {
    if (dynamicFrame.errorsCount > 0) {
      if (showAll) {
        dynamicFrame.errorsAsDynamicFrame.show()
      } else {
        dynamicFrame.errorsAsDynamicFrame.show(1)
      }
      throw new CircleJobException("%s had %d errors".format(jobName, dynamicFrame.errorsCount))
    }
  }

  class CircleJobException(message: String) extends SparkException(message) {

    def this(message: String, cause: Throwable) {
      this(message)
      initCause(cause)
    }

    def this(cause: Throwable) {
      this(Option(cause).map(_.toString).orNull, cause)
    }

    def this() {
      this(null: String)
    }
  }

  class CircleJobExpectedTestException(message: String) extends CircleJobException(message)
}
