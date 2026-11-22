package com.damianhoward.positionledger.consume

import com.damianhoward.positionledger.config.AppConfig
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

/**
 * One-shot operator tool (`position-ledger export-fills <topic>`): publishes the ledger's fills to
 * [targetTopic] as the records orderbook's egress emits, in ledger order and keyed by symbol as the
 * producer keys them, so another ledger can be rebuilt from them.
 *
 * It exists because the stream alone cannot rebuild this ledger. The broker deletes the oldest
 * fills under retention, so the ledger is the only complete copy, and a recovery that can only
 * replay what the topic still holds recovers a partial book. Exporting gives a rebuild a complete
 * stream to start from, and the recovery drill does exactly that: export, rebuild a second ledger
 * in a schema of its own, and compare it with this one.
 *
 * **It refuses to export onto [sourceTopic].** Every exported record lands at new stream
 * coordinates, so the ledger's coordinate key cannot recognise it. The `exec_id` index does, for
 * each fill carrying one; a fill recorded before execution ids existed carries none and would be
 * applied a second time. Exporting onto the live topic would double those fills in the live book.
 *
 * Every send is confirmed before the next, and a failed send aborts the run.
 */
class FillExport(
    private val fills: () -> List<Fill>,
    private val producer: Producer<String, String>,
    private val sourceTopic: String,
    private val targetTopic: String,
    private val confirmTimeout: Duration = DEFAULT_CONFIRM_TIMEOUT,
    private val log: (String) -> Unit = { exportLog.info(it) },
) : AutoCloseable {
    init {
        require(targetTopic != sourceTopic) {
            "refusing to export onto $sourceTopic: each copy lands at new coordinates, and a fill without an execId would apply twice"
        }
    }

    /** What one run did: [exported] records sent, of which [withoutExecId] carried no execution id. */
    data class Summary(
        val exported: Int,
        val withoutExecId: Int,
    )

    fun run(): Summary {
        var exported = 0
        var withoutExecId = 0
        for (fill in fills()) {
            producer.send(ProducerRecord(targetTopic, fill.symbol, envelope(fill))).get(confirmTimeout.toMillis(), TimeUnit.MILLISECONDS)
            exported++
            if (fill.execId == null) withoutExecId++
        }
        log("exported $exported fill(s) from the $sourceTopic ledger to $targetTopic; $withoutExecId without an execId")
        return Summary(exported, withoutExecId)
    }

    override fun close() = producer.close(CLOSE_TIMEOUT)

    companion object {
        val DEFAULT_CONFIRM_TIMEOUT: Duration = Duration.ofSeconds(10)
        private val CLOSE_TIMEOUT = Duration.ofSeconds(5)

        /**
         * The record orderbook's egress emits for [fill], which [Fill.parse] reads back to an equal
         * fill. The price is written as a string, as the producer writes it, so it stays exact; an
         * absent execId is left out rather than written as null, as on the records that predate it.
         */
        fun envelope(fill: Fill): String =
            buildJsonObject {
                put("v", Fill.SCHEMA_VERSION)
                fill.execId?.let { put("execId", it) }
                put("symbol", fill.symbol)
                put("price", fill.price.toPlainString())
                put("size", fill.size)
                put("makerOrderId", fill.makerOrderId)
                put("takerOrderId", fill.takerOrderId)
                put("aggressor", fill.aggressor.name)
                put("ts", fill.timeMillis)
            }.toString()

        /** An export of [config]'s fills topic ledger to [targetTopic], over a real producer. */
        fun create(
            config: AppConfig,
            fills: () -> List<Fill>,
            targetTopic: String,
        ): FillExport {
            val producerProps =
                Properties().apply {
                    put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers)
                    put(ProducerConfig.CLIENT_ID_CONFIG, "position-ledger-fill-export")
                    // One record per send, confirmed in order, so idempotence keeps a retried send
                    // from landing twice on the target.
                    put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
                    put(ProducerConfig.ACKS_CONFIG, "all")
                    put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000)
                    put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000)
                    put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000)
                }
            return FillExport(
                fills = fills,
                producer = KafkaProducer(producerProps, StringSerializer(), StringSerializer()),
                sourceTopic = config.fillsTopic,
                targetTopic = targetTopic,
            )
        }
    }
}

private val exportLog = LoggerFactory.getLogger(FillExport::class.java)
