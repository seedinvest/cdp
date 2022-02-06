package com.circle.data.utils

import org.apache.log4j.{LogManager, Logger}

trait Logging {
  implicit protected val logger: Logger = LogManager.getLogger(getClass.getName)
}
