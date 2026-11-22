package com.damianhoward.positionledger.consume

import com.damianhoward.orderbook.model.Side
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.RoundRobinPartitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
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
    private val targetTopic = "orderbook.fills.archive"

    private val aapl =
        Fill(
            symbol = "AAPL",
            price = BigDecimal("340.73000000"),
            size = 7,
            makerOrderId = 11,
            takerOrderId = 22,
            aggressor = Side.OFFER,
            timeMillis = 1_791_437_430_900,
            execId = "1791437000000-22",
        )

    // A fill recorded before the producer stamped ids, as the ledger holds it since V5.
    private val legacy =
        Fill(
            symbol = "MSFT",
            price = BigDecimal("101.5"),
            size = 3,
            makerOrderId = 5,
            takerOrderId = 6,
            aggressor = Side.BID,
            timeMillis = 1_000,
            execId = "legacy-orderbook.fills-0-3",
        )

    private val node = Node(1, "localhost", 9092)

    private fun producer(
        partitions: Int = 1,
        autoComplete: Boolean = true,
    ): MockProducer<String, String> {
        val cluster =
            Cluster(
                "test",
                listOf(node),
                (0 until partitions).map { PartitionInfo(targetTopic, it, node, arrayOf(node), arrayOf(node)) },
                emptySet(),
                emptySet(),
            )
        return MockProducer(cluster, autoComplete, RoundRobinPartitioner(), StringSerializer(), StringSerializer())
    }

    @Test
    fun `an envelope parses back to the fill it was written from`() {
        for (fill in listOf(aapl, legacy, aapl.copy(execId = null))) {
            assertEquals(fill, Fill.parse(FillExport.envelope(fill)))
        }
    }

    @Test
    fun `an absent execId is left out of the envelope, and the price stays an exact string`() {
        val envelope = FillExport.envelope(legacy.copy(execId = null))
        assertFalse(envelope.contains("execId"), envelope)
        assertTrue(envelope.contains(""""price":"101.5""""), envelope)
        assertTrue(FillExport.envelope(aapl).contains(""""price":"340.73000000""""))
    }

    @Test
    fun `exports every fill in ledger order, keyed by execId so compaction keeps one record per fill`() {
        val producer = producer()
        val logged = mutableListOf<String>()
        val summary = FillExport({ listOf(aapl, legacy) }, producer, sourceTopic, targetTopic, log = logged::add).run()

        assertEquals(FillExport.Summary(exported = 2), summary)
        val sent = producer.history()
        assertEquals(listOf(aapl.execId, legacy.execId), sent.map { it.key() })
        assertTrue(sent.all { it.topic() == targetTopic })
        assertEquals(listOf(aapl, legacy), sent.map { Fill.parse(it.value()) })
        assertTrue(logged.single().contains("exported 2 fill(s)"), logged.toString())
    }

    @Test
    fun `refuses to export onto the topic the ledger consumes`() {
        val failure =
            assertThrows(IllegalArgumentException::class.java) {
                FillExport({ listOf(aapl) }, producer(), sourceTopic, sourceTopic)
            }
        assertTrue(failure.message!!.contains("refusing to export onto $sourceTopic"), failure.message)
    }

    @Test
    fun `refuses a target with more than one partition, where ledger order would not hold`() {
        val producer = producer(partitions = 2)
        val failure =
            assertThrows(IllegalStateException::class.java) { FillExport({ listOf(aapl) }, producer, sourceTopic, targetTopic).run() }
        assertTrue(failure.message!!.contains("2 partitions"), failure.message)
        assertTrue(producer.history().isEmpty(), "nothing is sent to a target that cannot keep order")
    }

    @Test
    fun `refuses a fill without an execId, which compaction would collide`() {
        val producer = producer()
        assertThrows(IllegalStateException::class.java) {
            FillExport({ listOf(aapl, aapl.copy(execId = null)) }, producer, sourceTopic, targetTopic).run()
        }
        assertEquals(1, producer.history().size, "the export stops at the fill it cannot key")
    }

    @Test
    fun `a failed send stops the export and surfaces to the caller`() {
        val producer = producer(autoComplete = false)
        val export = FillExport({ listOf(aapl, legacy) }, producer, sourceTopic, targetTopic)
        var failure: Throwable? = null
        val running = Thread { failure = runCatching { export.run() }.exceptionOrNull() }
        running.start()
        while (producer.history().isEmpty()) Thread.sleep(5)
        producer.errorNext(RuntimeException("broker unavailable"))
        running.join(5_000)
        assertTrue(failure is ExecutionException, "got $failure")
        assertEquals(1, producer.history().size, "the second fill is never sent after the first failed")
    }
}
