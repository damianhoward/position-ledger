package com.damianhoward.positionledger.exposure

import com.damianhoward.positionledger.consume.ConsumerProgress
import com.damianhoward.positionledger.view.BreachEventWire
import com.damianhoward.positionledger.view.ExposureWire
import com.damianhoward.positionledger.view.SymbolExposureWire
import com.damianhoward.positionledger.view.toWire
import kotlinx.serialization.json.Json
import java.math.BigDecimal

/** Which ceiling a breach event refers to. */
enum class LimitKind { POSITION, NOTIONAL }

/**
 * One transition of a symbol's standing against a single limit: [breached] is true when the
 * limit was crossed, false when exposure dropped back under. [timeMillis] is the fill's own
 * timestamp, so replaying the stream rebuilds the same events.
 */
data class BreachEvent(
    val symbol: String,
    val kind: LimitKind,
    val breached: Boolean,
    val value: BigDecimal,
    val limit: BigDecimal,
    val timeMillis: Long,
)

/** A symbol's current exposure against both limits. Utilisations are 4-dp fractions of the limit. */
data class SymbolExposure(
    val symbol: String,
    val netQuantity: Long,
    val lastPrice: BigDecimal,
    val notional: BigDecimal,
    val positionUtilisation: BigDecimal,
    val notionalUtilisation: BigDecimal,
    val breached: Boolean,
) {
    fun utilisation(kind: LimitKind): BigDecimal =
        when (kind) {
            LimitKind.POSITION -> positionUtilisation
            LimitKind.NOTIONAL -> notionalUtilisation
        }
}

/**
 * The detector's whole view at one moment: per-symbol exposures, the bounded breach/clear history
 * (newest first), how many malformed records were skipped, and how far along the stream this view
 * has read ([progress], null before the first fill). [toJson] follows the snapshot's wire
 * convention — one line, exact decimals as plain JSON numbers.
 */
data class ExposureReport(
    val limits: RiskLimits,
    val symbols: List<SymbolExposure>,
    val events: List<BreachEvent>,
    val malformed: Long,
    /** Breaches counted since start. Monotonic, unlike [events], which the deque evicts. */
    val breaches: Long = 0,
    val progress: ConsumerProgress? = null,
) {
    val breachedSymbols: Int get() = symbols.count { it.breached }

    /**
     * The worst utilisation across symbols for one ceiling, null with nothing to measure. Above 1
     * means breached.
     *
     * A maximum rather than a series per symbol: labelled by symbol, cardinality would grow with
     * what trades rather than with what is configured.
     */
    fun worstUtilisation(kind: LimitKind): BigDecimal? = symbols.map { it.utilisation(kind) }.maxOrNull()

    fun toJson(): String = Json.encodeToString(toWire())

    internal fun toWire(): ExposureWire =
        ExposureWire(
            maxPosition = limits.maxAbsPosition,
            maxNotional = limits.maxNotional,
            symbols =
                symbols.map {
                    SymbolExposureWire(
                        it.symbol,
                        it.netQuantity,
                        it.lastPrice,
                        it.notional,
                        it.positionUtilisation,
                        it.notionalUtilisation,
                        it.breached,
                    )
                },
            events = events.map { BreachEventWire(it.symbol, it.kind.toString(), it.breached, it.value, it.limit, it.timeMillis) },
            malformed = malformed,
            breaches = breaches,
            progress = progress?.toWire(),
        )
}
