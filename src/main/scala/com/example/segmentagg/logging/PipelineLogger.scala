package com.example.segmentagg.logging

import java.time.Instant

/**
 * Abstraction over pipeline logging.
 *
 * Consumers depend on this trait rather than on `println` directly,
 * making it easy to swap in SLF4J, a test spy, or any other backend.
 */
trait PipelineLogger {
  def info(message: String): Unit
  def warn(message: String): Unit
  def error(message: String, cause: Option[Throwable] = None): Unit
}

/**
 * Default implementation that writes structured log lines to stdout.
 */
final class ConsolePipelineLogger(tag: String) extends PipelineLogger {

  override def info(message: String): Unit =
    println(s"[${Instant.now()}] [$tag] INFO  $message")

  override def warn(message: String): Unit =
    println(s"[${Instant.now()}] [$tag] WARN  $message")

  override def error(message: String, cause: Option[Throwable] = None): Unit = {
    println(s"[${Instant.now()}] [$tag] ERROR $message")
    cause.foreach(_.printStackTrace(System.err))
  }
}

object PipelineLogger {

  /** Convenience factory for the default console logger. */
  def console(tag: String): PipelineLogger = new ConsolePipelineLogger(tag)
}

