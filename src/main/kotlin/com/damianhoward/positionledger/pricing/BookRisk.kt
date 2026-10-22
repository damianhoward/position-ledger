package com.damianhoward.positionledger.pricing

import com.damianhoward.positionledger.view.BookWire
import com.damianhoward.positionledger.view.SymbolRiskWire
import com.damianhoward.riskengine.report.RiskReport
import kotlinx.serialization.json.Json
import java.math.BigDecimal

/** One symbol's validated single-underlier report, beside the open its day PnL measures from. */
data class SymbolRisk(
    val symbol: String,
    val openPrice: BigDecimal?,
    val report: RiskReport,
)

/**
 * The whole book's risk: every position gets its own report, plus the sums that are honest to
 * sum. Valuation, gross notional, and day PnL are currency amounts and add; Greeks and VaR stay
 * per-symbol on purpose — summing share-count deltas across underliers, or taking a quantile
 * across independently-scenarioed symbols, would label numbers the library's single-underlier
 * market model does not price.
 */
data class BookRisk(
    val symbols: List<SymbolRisk>,
) {
    val valuation: BigDecimal = symbols.sumOf { it.report.valuation.amount }

    /** Σ|per-symbol valuation| — for a cash-equity book each term is |quantity × spot|. */
    val grossNotional: BigDecimal =
        symbols.sumOf {
            it.report.valuation.amount
                .abs()
        }

    /** Sum of the day PnL of every symbol that has traded today, or null when none has. */
    val dayPnl: BigDecimal? =
        symbols
            .mapNotNull {
                it.report.pnl
                    ?.actual
                    ?.amount
            }.takeIf { it.isNotEmpty() }
            ?.reduce(BigDecimal::add)

    internal fun toWire(): BookWire =
        BookWire(
            valuation = valuation,
            grossNotional = grossNotional,
            dayPnl = dayPnl,
            symbols = symbols.map { SymbolRiskWire(it.symbol, it.openPrice, Json.parseToJsonElement(it.report.toJson())) },
        )
}
