package com.circle.data.utils

import org.apache.commons.lang3.StringUtils.isNoneEmpty

import java.nio.file.Paths

object PathUtils {

  /**
   * Assemble a path by joining path parts with `/` while respecting a protocol part if it exists in the head.
   *
   * ["a", "b"] -> "a/b"
   * ["/a/", "b"] -> "/a/b"
   *
   * End slashes are removed:
   *
   * ["/a/", "b/"] -> "/a/b"   end slashes are removed
   *
   * Protocols are preserved:
   *
   * ["s3://bucket", "path/to/", "artifact.txt"] -> "s3://bucket/path/to/artifact.txt"
   *
   */
  def join(parts: String*): String = {
    val first = parts.head
    if (first.contains("://")) {
      val head = first.substring(0, first.indexOf("://") + 3)
      val protocol = if (head.equals("file://")) head + "/" else head // special handling for `file://` protocol
      val tail = Paths.get(first.substring(first.indexOf("://") + 3), parts.tail: _*).toString
      val path = if (tail.startsWith("/")) tail.substring(1) else tail // remove the leading `/` if present
      protocol + path
    } else {
      Paths.get(parts.head, parts.tail: _*).toString
    }
  }

  def parseBucketAndPrefix(url: String): (String, String) = {
    if (url.startsWith("s3")) {
      val slug = url.substring(url.indexOf("://") + 3)
      val bucket = slug.substring(0, slug.indexOf("/"))
      val path = slug.substring(slug.indexOf("/"))
      val postFix = if (path.endsWith("/")) "/" else ""
      val prefix = path.split("/").filter(isNoneEmpty(_)).mkString("/") + postFix
      (bucket, prefix)
    } else throw new IllegalArgumentException(s"invalid s3 url '$url'")
  }
}
