package com.codesolutions.flink

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class EventStreamJobSpec extends AnyFunSuite with Matchers {

  test("enrichEvent should add timestamp and metadata") {
    val input = """{"id":1,"type":"legacy-order"}"""
    val result = EventStreamJob.enrichEvent(input)

    result should include ("ENRICHED[")
    result should include (input)
    result should include ("processed_by=flink-kafka-base")
    result should include ("org=CodeSolutions")
  }

  test("enrichEvent should handle empty input") {
    val result = EventStreamJob.enrichEvent("")
    result should include ("ENRICHED[")
    result should include ("from_legacy_event: ")
  }
}
