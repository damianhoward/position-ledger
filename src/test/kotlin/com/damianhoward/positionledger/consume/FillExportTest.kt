package com.damianhoward.positionledger.consume

import com.damianhoward.orderbook.model.Side
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.util.concurrent.ExecutionException

class FillExportTest {
    private val sourceTopic = "orderbook.fills"
    private val targetTopic = "orderbook.fills.drill"

    private val withId =
        Fill(
            symbol = "AAPL",
            price = BigDecimal("340.73000000"),
            size = 7,
            makerOrderId = 11,
            takerOrderId = 22,
            aggressor = Side.OFFER,
            timeMillis = 1_791_437_430_900,
            execId = "AAPL-1791437430900-22",
        )

    // Recorded before the producer carried execution ids: the case that makes the source-topic
    // guard necessary.
    private val withoutId =
        Fill(
            symbol = "MSFT",
            price = BigDecimal("101.5"),
            size = 3,
            makerOrderId = 5,
            takerOrderId = 6,
            aggressor = Side.BID,
            timeMillis = 1_000,
        )

    private fun producer(autoComplete: Boolean = true) = MockProducer(autoComplete, null, StringSerializer(), StringSerializer())

    @Test
    fun `an envelope parses back to the fill it was written from, with and without an execId`() {
        for (fill in listOf(withId, withoutId)) {
            assertEquals(fill, Fill.parse(FillExport.envelope(fill)))
        }
    }

    @Test
    fun `an absent execId is left out of the envelope, and the price stays an exact string`() {
        val envelope = FillExport.envelope(withoutId)
        assertFalse(envelope.contains("execId"), envelope)
        assertTrue(envelope.contains(""""price":"101.5""""), envelope)
        assertTrue(FillExport.envelope(withId).contains(""""price":"340.73000000""""))
    }

    @Test
    fun `exports every fill to the target in ledger order, keyed by symbol`() {
        val producer = producer()
        val logged = mutableListOf<String>()
        val summary = FillExport({ listOf(withId, withoutId, withId) }, producer, sourceTopic, targetTopic, log = logged::add).run()

        assertEquals(FillExport.Summary(exported = 3, withoutExecId = 1), summary)
        val sent = producer.history()
        assertEquals(listOf("AAPL", "MSFT", "AAPL"), sent.map { it.key() }, "keyed as the producer keys, so per-symbol order holds")
        assertTrue(sent.all { it.topic() == targetTopic })
        assertEquals(listOf(withId, withoutId, withId), sent.map { Fill.parse(it.value()) })
        assertTrue(logged.single().contains("1 without an execId"), logged.toString())
    }

    @Test
    fun `refuses to export onto the topic the ledger came from`() {
        val failure =
            assertThrows(IllegalArgumentException::class.java) {
                FillExport({ listOf(withoutId) }, producer(), sourceTopic, sourceTopic)
            }
        assertTrue(failure.message!!.contains("refusing to export onto $sourceTopic"), failure.message)
    }

    @Test
    fun `a failed send stops the export rather than skipping the fill`() {
        val producer = producer(autoComplete = false)
        val export = FillExport({ listOf(withId, withoutId) }, producer, sourceTopic, targetTopic)
        val running = Thread { runCatching { export.run() } }
        running.start()
        while (producer.history().isEmpty()) Thread.sleep(5)
        producer.errorNext(RuntimeException("broker unavailable"))
        running.join(5_000)
        assertEquals(1, producer.history().size, "the second fill is never sent after the first failed")
    }

    @Test
    fun `a failed send surfaces to the caller`() {
        val producer = producer(autoComplete = false)
        val export = FillExport({ listOf(withId) }, producer, sourceTopic, targetTopic)
        var failure: Throwable? = null
        val running = Thread { failure = runCatching { export.run() }.exceptionOrNull() }
        running.start()
        while (producer.history().isEmpty()) Thread.sleep(5)
        producer.errorNext(RuntimeException("broker unavailable"))
        running.join(5_000)
        assertTrue(failure is ExecutionException, "got $failure")
    }
}
