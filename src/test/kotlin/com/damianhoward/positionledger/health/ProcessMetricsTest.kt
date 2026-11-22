package com.damianhoward.positionledger.health

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.lang.management.ManagementFactory
import java.lang.management.MemoryPoolMXBean
import java.lang.management.MemoryType
import java.lang.management.MemoryUsage
import javax.management.ObjectName

/**
 * The two heap gauges exist to answer "what ceiling does this service need" without a fast series,
 * so what is worth asserting is the shape of the answer when the JVM cannot give one — a heap that
 * has never been collected, and a pool that does not report collection usage at all. Both must be
 * absent rather than zero, because zero is a claim about the heap and absence is a claim about the
 * measurement.
 */
class ProcessMetricsTest {
    /** Only the four methods under test are real; the rest of the interface is never reached. */
    private class Pool(
        private val poolType: MemoryType,
        private val peak: Long,
        private val collection: MemoryUsage?,
        private val name: String = "stub",
        private val used: Long = 1,
        private val max: Long = 1,
    ) : MemoryPoolMXBean {
        override fun getType() = poolType

        override fun getPeakUsage() = MemoryUsage(0, peak, peak, peak)

        override fun getCollectionUsage() = collection

        override fun getName() = name

        override fun getUsage() = MemoryUsage(0, used, used, max)

        override fun isValid() = true

        override fun getMemoryManagerNames() = arrayOf("stub")

        override fun getUsageThreshold() = 0L

        override fun setUsageThreshold(threshold: Long) = Unit

        override fun isUsageThresholdExceeded() = false

        override fun getUsageThresholdCount() = 0L

        override fun isUsageThresholdSupported() = false

        override fun getCollectionUsageThreshold() = 0L

        override fun setCollectionUsageThreshold(threshold: Long) = Unit

        override fun isCollectionUsageThresholdExceeded() = false

        override fun getCollectionUsageThresholdCount() = 0L

        override fun isCollectionUsageThresholdSupported() = false

        override fun resetPeakUsage() = Unit

        override fun getObjectName(): ObjectName = ObjectName("java.lang:type=MemoryPool,name=stub")
    }

    private fun metricsWith(pools: List<MemoryPoolMXBean>) =
        ProcessMetrics(
            runtime = ManagementFactory.getRuntimeMXBean(),
            memory = ManagementFactory.getMemoryMXBean(),
            threads = ManagementFactory.getThreadMXBean(),
            collectors = ManagementFactory.getGarbageCollectorMXBeans(),
            pools = pools,
        ).render()

    private fun usage(used: Long) = MemoryUsage(0, used, used, used)

    @Test
    fun `peak and post-collection are summed across heap pools`() {
        val metrics =
            metricsWith(
                listOf(
                    Pool(MemoryType.HEAP, peak = 100, collection = usage(30)),
                    Pool(MemoryType.HEAP, peak = 50, collection = usage(20)),
                ),
            )
        assertTrue(metrics.contains("position_ledger_jvm_heap_peak_bytes 150"), metrics)
        assertTrue(metrics.contains("position_ledger_jvm_heap_post_gc_bytes 50"), metrics)
    }

    @Test
    fun `non-heap pools are excluded from both`() {
        // Metaspace is not governed by the heap ceiling, so counting it would inflate the number
        // whose only purpose is comparison against -Xmx.
        val metrics =
            metricsWith(
                listOf(
                    Pool(MemoryType.HEAP, peak = 100, collection = usage(30)),
                    Pool(MemoryType.NON_HEAP, peak = 900, collection = usage(800)),
                ),
            )
        assertTrue(metrics.contains("position_ledger_jvm_heap_peak_bytes 100"), metrics)
        assertTrue(metrics.contains("position_ledger_jvm_heap_post_gc_bytes 30"), metrics)
    }

    @Test
    fun `a heap not yet collected publishes no live set at all`() {
        val metrics = metricsWith(listOf(Pool(MemoryType.HEAP, peak = 100, collection = null)))
        assertTrue(metrics.contains("position_ledger_jvm_heap_peak_bytes 100"), metrics)
        // Zero would read as an empty heap; the fact is that nothing has established a live set.
        assertFalse(metrics.contains("position_ledger_jvm_heap_post_gc_bytes"), metrics)
    }

    @Test
    fun `one silent pool withdraws the live set rather than under-reporting it`() {
        // A partial sum would look like a smaller live set, which is the direction that talks a
        // ceiling down — the error this pair of gauges exists to prevent.
        val metrics =
            metricsWith(
                listOf(
                    Pool(MemoryType.HEAP, peak = 100, collection = usage(30)),
                    Pool(MemoryType.HEAP, peak = 50, collection = null),
                ),
            )
        assertTrue(metrics.contains("position_ledger_jvm_heap_peak_bytes 150"), metrics)
        assertFalse(metrics.contains("position_ledger_jvm_heap_post_gc_bytes"), metrics)
    }

    @Test
    fun `no heap pools publishes neither gauge`() {
        val metrics = metricsWith(listOf(Pool(MemoryType.NON_HEAP, peak = 900, collection = usage(800))))
        assertFalse(metrics.contains("position_ledger_jvm_heap_peak_bytes"), metrics)
        assertFalse(metrics.contains("position_ledger_jvm_heap_post_gc_bytes"), metrics)
    }

    @Test
    fun `the real JVM answers both, and the peak is not below the live set`() {
        // Against the actual MXBeans rather than stubs, so the gauges are known to work on the JVM
        // the service runs on and not only against the shapes this test imagines.
        System.gc()
        val metrics = ProcessMetrics().render()
        val peak = valueOf(metrics, "position_ledger_jvm_heap_peak_bytes")
        val live = valueOf(metrics, "position_ledger_jvm_heap_post_gc_bytes")
        assertTrue(peak != null && peak > 0, metrics)
        assertTrue(live != null && live > 0, metrics)
        assertTrue(peak!! >= live!!, "peak $peak should not be below live set $live")
    }

    @Test
    fun `no series is declared twice`() {
        // The pair added here is the first chance to publish the same name from two places, and a
        // duplicate is what a collector reads as a series disagreeing with itself.
        val names =
            ProcessMetrics()
                .render()
                .lineSequence()
                .filter { it.startsWith("# TYPE ") }
                .map { it.split(' ')[2] }
                .toList()
        assertEquals(names.size, names.toSet().size, names.toString())
    }

    @Test
    fun `the code cache is summed across its segments, beside its ceiling`() {
        // Segmented, as every unit runs it: profiled and non-profiled nmethods plus the non-nmethod
        // heap. Metaspace is non-heap too and must not be counted.
        val metrics =
            metricsWith(
                listOf(
                    Pool(MemoryType.HEAP, peak = 100, collection = usage(30)),
                    Pool(MemoryType.NON_HEAP, peak = 0, collection = null, name = "CodeHeap 'profiled nmethods'", used = 7, max = 60),
                    Pool(MemoryType.NON_HEAP, peak = 0, collection = null, name = "CodeHeap 'non-profiled nmethods'", used = 5, max = 60),
                    Pool(MemoryType.NON_HEAP, peak = 0, collection = null, name = "CodeHeap 'non-nmethods'", used = 1, max = 8),
                    Pool(MemoryType.NON_HEAP, peak = 0, collection = null, name = "Metaspace", used = 900, max = -1),
                ),
            )
        assertTrue(metrics.contains("position_ledger_jvm_code_cache_used_bytes 13"), metrics)
        assertTrue(metrics.contains("position_ledger_jvm_code_cache_max_bytes 128"), metrics)
    }

    @Test
    fun `an unsegmented code cache is read from its single pool`() {
        val metrics =
            metricsWith(listOf(Pool(MemoryType.NON_HEAP, peak = 0, collection = null, name = "CodeCache", used = 9, max = 128)))
        assertTrue(metrics.contains("position_ledger_jvm_code_cache_used_bytes 9"), metrics)
        assertTrue(metrics.contains("position_ledger_jvm_code_cache_max_bytes 128"), metrics)
    }

    @Test
    fun `a code cache ceiling that is undefined is absent, not under-counted`() {
        val metrics =
            metricsWith(
                listOf(
                    Pool(MemoryType.NON_HEAP, peak = 0, collection = null, name = "CodeHeap 'profiled nmethods'", used = 7, max = 60),
                    Pool(MemoryType.NON_HEAP, peak = 0, collection = null, name = "CodeHeap 'non-nmethods'", used = 1, max = -1),
                ),
            )
        assertTrue(metrics.contains("position_ledger_jvm_code_cache_used_bytes 8"), metrics)
        assertFalse(metrics.contains("position_ledger_jvm_code_cache_max_bytes"), metrics)
    }

    @Test
    fun `a running JVM reports its code cache`() {
        val metrics = ProcessMetrics().render()
        assertTrue(metrics.contains("position_ledger_jvm_code_cache_used_bytes "), metrics)
        assertTrue(metrics.contains("position_ledger_jvm_code_cache_max_bytes "), metrics)
    }

    private fun valueOf(
        metrics: String,
        name: String,
    ): Long? =
        metrics
            .lineSequence()
            .firstOrNull { it.startsWith("$name ") }
            ?.substringAfter(' ')
            ?.trim()
            ?.toLong()
}
