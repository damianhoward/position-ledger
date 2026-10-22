package com.damianhoward.positionledger.view

import com.damianhoward.positionledger.consume.ConsumerProgress
import com.damianhoward.positionledger.exposure.ExposureReport
import com.damianhoward.positionledger.position.Position
import com.damianhoward.positionledger.pricing.BookRisk
import kotlinx.serialization.json.Json

/**
 * Everything the desk renders, at one moment: the booked [positions], the [book] risk —
 * one report per position plus the sums that are honest to sum (null before anything has
 * traded) — the [exposure] view the detector derives, and where each consumer path sits on the
 * stream. The two paths are independent consumers, so the `sync` block states whether they
 * describe the same stream position instead of leaving the reader to assume it, along with the
 * poison records dead-lettered this session ([deadLetters]). [toJson] is the wire contract
 * `/api/state` and the SSE stream both carry; `v:2` replaced the single-instrument `report`
 * with the whole-book shape.
 */
data class LedgerSnapshot(
    val positions: List<Position>,
    val book: BookRisk?,
    val exposure: ExposureReport,
    val positionsProgress: ConsumerProgress? = null,
    val duplicates: Long = 0,
    val deadLetters: Long = 0,
) {
    /** True when both paths have read to the same offset (or neither has seen a fill). */
    val coherent: Boolean get() = positionsProgress?.offset == exposure.progress?.offset

    fun toJson(): String =
        Json.encodeToString(
            SnapshotWire(
                v = 2,
                positions = positions.map { PositionWire(it.symbol, it.quantity, it.lastPrice, it.lastTimeMillis) },
                book = book?.toWire(),
                exposure = exposure.toWire(),
                sync =
                    SyncWire(
                        positions = positionsProgress?.toWire(),
                        exposure = exposure.progress?.toWire(),
                        coherent = coherent,
                        duplicatesDropped = duplicates,
                        deadLetters = deadLetters,
                    ),
            ),
        )
}

internal fun ConsumerProgress.toWire(): ProgressWire = ProgressWire(offset, fillTimeMillis)
