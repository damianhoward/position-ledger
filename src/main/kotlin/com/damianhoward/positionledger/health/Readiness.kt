package com.damianhoward.positionledger.health

import com.damianhoward.positionledger.consume.ConsumerHealth
import com.damianhoward.positionledger.consume.ConsumerProgress
import com.damianhoward.positionledger.exposure.ExposureReport
import com.damianhoward.positionledger.exposure.LimitKind
import com.damianhoward.positionledger.position.Divergence
import com.damianhoward.positionledger.position.Inconclusive
import com.damianhoward.positionledger.position.Reconciliation
import com.damianhoward.positionledger.position.ReconciliationChecker
import com.damianhoward.positionledger.position.View
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.time.Clock
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

/**
 * The service's operational truth, aggregated for `/readyz`: every consumer thread alive,
 * assigned, and polling recently; the database answering; the two views describing the same
 * stream position; the dead-letter counters in the open. `/healthz` proves the web process
 * answers — this proves the system is doing its job, so a server whose projections disagree is
 * not safe to serve, whatever the process state says.
 *
 * View coherence gets a grace window: the positions and exposure consumers are independent, so
 * mid-burst they legitimately sit a few records apart for well under a second. Offsets that stay
 * unequal past [coherenceGrace] mean a view is stuck, and the probe goes 503 naming both
 * positions. Past dead-letter failures are reported but do not fail the probe (an unacknowledged
 * send already fails fast at the moment it happens).
 *
 * Reconciliation asks the question coherence cannot. Equal offsets prove the two views have read
 * the same amount of stream, not that either holds the right number; a projection that drifted
 * from its ledger sits at the correct offset with the wrong quantity. So the probe also fails when
 * any of the three derived views stops agreeing with the fills it comes from, or when the check
 * establishing that has gone stale.
 *
 * A divergence gets no grace window. The `positions` table is read at one instant from one
 * statement, so it is either true or it is a defect, and an in-memory view is only ever judged at
 * a matching stream offset — there is no race left that makes a divergence briefly legitimate.
 * Being unable to judge a view is the separate case, and it does get a window: an in-memory
 * projection mid-flight when a pass runs is ordinary, one that can never be caught up with is a
 * check that has quietly stopped asserting anything. See [inconclusiveGrace].
 */
class Readiness(
    private val consumers: List<ConsumerHealth>,
    private val databaseOk: () -> Boolean,
    private val deadLettersPublished: () -> Long,
    private val deadLettersFailed: () -> Long,
    private val positionsView: () -> ConsumerProgress?,
    // The whole report, not just its progress: offsets and breach counts read separately could be
    // sampled a moment apart, which is what the single snapshot exists to rule out.
    private val exposureReport: () -> ExposureReport?,
    private val reconciliation: () -> Reconciliation?,
    private val maxPollAge: Duration = MAX_POLL_AGE,
    private val coherenceGrace: Duration = COHERENCE_GRACE,
    private val maxReconciliationAge: Duration = ReconciliationChecker.MAX_AGE,
    private val inconclusiveGrace: Duration = INCONCLUSIVE_GRACE,
    private val process: ProcessMetrics = ProcessMetrics(),
    private val clock: Clock = Clock.systemUTC(),
) {
    data class Probe(
        val ready: Boolean,
        val json: String,
    )

    @Volatile
    private var incoherentSinceMillis: Long? = null

    /** When each view last became unjudgeable, cleared the moment it is judged again. */
    private val inconclusiveSinceMillis = ConcurrentHashMap<View, Long>()

    /**
     * One evaluation of everything above, which both `/readyz` and `/metrics` render.
     *
     * `/readyz` and `/metrics` must never disagree about the same condition, and the strongest
     * way to guarantee that is to leave them nothing to disagree with: one set of numbers, read at
     * one instant, rendered twice.
     */
    private data class Snapshot(
        val ready: Boolean,
        val consumers: List<ConsumerState>,
        val databaseOk: Boolean,
        val views: ViewsState,
        val exposure: ExposureReport?,
        val ledger: LedgerState,
        val deadLettersPublished: Long,
        val deadLettersFailed: Long,
    )

    private data class ConsumerState(
        val name: String,
        val ok: Boolean,
        val threadAlive: Boolean,
        val assigned: Boolean,
        val pollAgeMillis: Long?,
        val fatal: String?,
    )

    private data class ViewsState(
        val ok: Boolean,
        val positionsOffset: Long?,
        val exposureOffset: Long?,
        val coherent: Boolean,
        val incoherentForMillis: Long?,
    )

    private data class LedgerState(
        val ok: Boolean,
        val ageMillis: Long?,
        val fresh: Boolean,
        val views: List<ViewLedgerState>,
    )

    private data class ViewLedgerState(
        val view: View,
        val ok: Boolean,
        val symbolsChecked: Int,
        val divergences: List<Divergence>,
        val inconclusive: Inconclusive?,
        val inconclusiveForMillis: Long?,
    )

    private fun snapshot(): Snapshot {
        val consumerStates = consumers.map(::consumerState)
        val database = databaseOk()
        val exposure = exposureReport()
        val views = viewsState(exposure)
        val ledger = ledgerState()
        return Snapshot(
            ready = database && views.ok && ledger.ok && consumerStates.all { it.ok },
            consumers = consumerStates,
            databaseOk = database,
            views = views,
            exposure = exposure,
            ledger = ledger,
            deadLettersPublished = deadLettersPublished(),
            deadLettersFailed = deadLettersFailed(),
        )
    }

    fun probe(): Probe {
        val now = snapshot()
        val body =
            ProbeBody(
                ready = now.ready,
                consumers =
                    now.consumers.associate {
                        it.name to ConsumerBody(it.ok, it.threadAlive, it.assigned, it.pollAgeMillis, it.fatal)
                    },
                database = DatabaseBody(now.databaseOk),
                views = with(now.views) { ViewsBody(ok, positionsOffset, exposureOffset, coherent, incoherentForMillis) },
                ledger = ledgerBody(now.ledger),
                deadLetters = DeadLettersBody(now.deadLettersPublished, now.deadLettersFailed),
            )
        return Probe(now.ready, Json.encodeToString(body))
    }

    /**
     * The conservation check's standing verdict, per derived view. Divergences are named
     * individually with both quantities: an operator seeing 503 needs the view, the symbol and the
     * size of the gap, and there is nothing secret in any of them — they are the same numbers the
     * desk already serves publicly.
     */
    private fun ledgerState(): LedgerState {
        val latest = reconciliation()
        val ageMillis = latest?.let { clock.millis() - it.checkedAtMillis }
        val fresh = ageMillis != null && ageMillis <= maxReconciliationAge.toMillis()
        val views = latest?.verdicts.orEmpty().map(::viewLedgerState)
        return LedgerState(
            ok = fresh && views.all { it.ok },
            ageMillis = ageMillis,
            fresh = fresh,
            views = views,
        )
    }

    /**
     * One view's contribution. An inconclusive verdict is not a pass and not a failure: it starts
     * a clock, and only outlasting [inconclusiveGrace] makes the probe unready. Judging resets it,
     * so an ordinary mid-flight view never accumulates.
     */
    private fun viewLedgerState(verdict: com.damianhoward.positionledger.position.ViewVerdict): ViewLedgerState {
        val inconclusiveFor =
            if (verdict.inconclusive == null) {
                inconclusiveSinceMillis.remove(verdict.view)
                null
            } else {
                val since = inconclusiveSinceMillis.computeIfAbsent(verdict.view) { clock.millis() }
                clock.millis() - since
            }
        return ViewLedgerState(
            view = verdict.view,
            ok = verdict.divergences.isEmpty() && (inconclusiveFor == null || inconclusiveFor <= inconclusiveGrace.toMillis()),
            symbolsChecked = verdict.symbolsChecked,
            divergences = verdict.divergences,
            inconclusive = verdict.inconclusive,
            inconclusiveForMillis = inconclusiveFor,
        )
    }

    private fun ledgerBody(state: LedgerState): LedgerBody =
        LedgerBody(
            ok = state.ok,
            checkedAgeMillis = state.ageMillis,
            views =
                state.views.associate { view ->
                    view.view.label to
                        ViewLedgerBody(
                            ok = view.ok,
                            symbolsChecked = view.symbolsChecked,
                            divergences =
                                view.divergences.map {
                                    DivergenceBody(it.symbol, it.ledgerQuantity, it.viewQuantity, it.difference)
                                },
                            inconclusive =
                                view.inconclusive?.let {
                                    InconclusiveBody(it.viewOffset, it.ledgerOffset, view.inconclusiveForMillis)
                                },
                        )
                },
        )

    private fun viewsState(report: ExposureReport?): ViewsState {
        val positions = positionsView()?.offset
        val exposure = report?.progress?.offset
        val coherent = positions == exposure
        val incoherentFor =
            if (coherent) {
                incoherentSinceMillis = null
                null
            } else {
                val since = incoherentSinceMillis ?: clock.millis().also { incoherentSinceMillis = it }
                clock.millis() - since
            }
        return ViewsState(
            ok = coherent || incoherentFor!! <= coherenceGrace.toMillis(),
            positionsOffset = positions,
            exposureOffset = exposure,
            coherent = coherent,
            incoherentForMillis = incoherentFor,
        )
    }

    private fun consumerState(health: ConsumerHealth): ConsumerState {
        val pollAge = health.pollAgeMillis()
        val polling = pollAge != null && pollAge <= maxPollAge.toMillis()
        return ConsumerState(
            name = health.name,
            ok = health.threadAlive && health.assigned && polling && health.fatal == null,
            threadAlive = health.threadAlive,
            assigned = health.assigned,
            pollAgeMillis = pollAge,
            fatal = health.fatal,
        )
    }

    /**
     * The same snapshot in Prometheus text format, for trend and post-incident reconstruction.
     *
     * Every series here is a number this process computes, never text it was handed: no exception
     * message, no file path, no rejected input. The endpoint is unauthenticated, so anything it
     * echoes back is published, and it costs nothing to keep to numbers — a metric is a number by
     * definition and the
     * temptation this rules out is the `_info` series carrying a failure string as a label.
     *
     * Divergences are counted rather than named per symbol. The probe names them because an
     * operator reading a 503 needs to know which symbol; a time series labelled by symbol would
     * instead create one series per symbol that has ever diverged and keep it forever, which is
     * how a small estate's metrics bill becomes someone's afternoon.
     *
     * Ages are seconds, not milliseconds. Prometheus convention is base units, and every
     * dashboard, alert expression and function assumes it.
     */
    fun metrics(): String {
        val now = snapshot()
        val out = StringBuilder()

        fun emit(
            name: String,
            help: String,
            type: String,
            samples: List<Pair<String, Any>>,
        ) {
            if (samples.isEmpty()) return
            out
                .append("# HELP ")
                .append(name)
                .append(' ')
                .append(help)
                .append('\n')
            out
                .append("# TYPE ")
                .append(name)
                .append(' ')
                .append(type)
                .append('\n')
            for ((labels, value) in samples) {
                out
                    .append(name)
                    .append(labels)
                    .append(' ')
                    .append(value)
                    .append('\n')
            }
        }

        fun gauge(
            name: String,
            help: String,
            value: Any,
        ) = emit(name, help, "gauge", listOf("" to value))

        gauge("position_ledger_ready", "Whether every readiness condition below currently holds.", now.ready.toInt())
        gauge("position_ledger_database_up", "Whether the database answered its check.", now.databaseOk.toInt())

        emit(
            "position_ledger_consumer_up",
            "Whether a consumer thread is alive, assigned and polling within its budget.",
            "gauge",
            now.consumers.map { """{consumer="${it.name}"}""" to it.ok.toInt() },
        )
        emit(
            "position_ledger_consumer_poll_age_seconds",
            "Seconds since a consumer last completed a poll. Absent before its first poll.",
            "gauge",
            now.consumers.mapNotNull { c -> c.pollAgeMillis?.let { """{consumer="${c.name}"}""" to seconds(it) } },
        )
        emit(
            "position_ledger_view_offset",
            "Stream offset each projection has applied.",
            "gauge",
            listOfNotNull(
                now.views.positionsOffset?.let { """{view="positions"}""" to it },
                now.views.exposureOffset?.let { """{view="exposure"}""" to it },
            ),
        )
        gauge(
            "position_ledger_views_coherent",
            "Whether the projections have applied the same amount of stream.",
            now.views.coherent.toInt(),
        )
        // Judged views only. A view the pass could not judge publishes no verdict rather than a
        // zero: pinned at 0 it reads as a standing divergence, pinned at 1 as an assertion nothing
        // actually made. The inconclusive gauge below is always present, so absence here is
        // readable rather than ambiguous — the same rule that keeps the egress counters absent
        // until a producer is wired.
        val judged = now.ledger.views.filter { it.inconclusive == null }
        emit(
            "position_ledger_ledger_agrees",
            "Whether a derived view equals the signed sum of the ledger fills behind it.",
            "gauge",
            judged.map { """{view="${it.view.label}"}""" to it.divergences.isEmpty().toInt() },
        )
        emit(
            "position_ledger_ledger_divergent_symbols",
            "Symbols whose view quantity disagrees with the fills it derives from.",
            "gauge",
            judged.map { """{view="${it.view.label}"}""" to it.divergences.size },
        )
        emit(
            "position_ledger_ledger_check_inconclusive",
            "Whether a view sat at a different stream offset than the ledger and could not be judged.",
            "gauge",
            now.ledger.views.map { """{view="${it.view.label}"}""" to (it.inconclusive != null).toInt() },
        )
        now.ledger.ageMillis?.let {
            gauge(
                "position_ledger_ledger_check_age_seconds",
                "Seconds since the conservation check last completed. Staleness fails readiness.",
                seconds(it),
            )
        }
        now.exposure?.let { exposure ->
            emit(
                "position_ledger_exposure_breaches_total",
                "Ceiling crossings counted since start. Detected after the fill, never refused before it.",
                "counter",
                listOf("" to exposure.breaches),
            )
            gauge(
                "position_ledger_exposure_symbols_breached",
                "Symbols whose exposure is currently over either ceiling.",
                exposure.breachedSymbols,
            )
            emit(
                "position_ledger_exposure_utilisation_ratio",
                "Worst exposure against its ceiling across symbols. Above 1 is a standing breach.",
                "gauge",
                listOfNotNull(
                    exposure.worstUtilisation(LimitKind.POSITION)?.let { """{limit="position"}""" to it.toPlainString() },
                    exposure.worstUtilisation(LimitKind.NOTIONAL)?.let { """{limit="notional"}""" to it.toPlainString() },
                ),
            )
        }

        emit(
            "position_ledger_dead_letters_published_total",
            "Records published to the dead-letter topic because they can never apply.",
            "counter",
            listOf("" to now.deadLettersPublished),
        )
        emit(
            "position_ledger_dead_letters_failed_total",
            "Dead-letter publishes the broker did not acknowledge.",
            "counter",
            listOf("" to now.deadLettersFailed),
        )

        // Appended, not interleaved: these are process facts rather than readiness conditions, and
        // keeping them in their own block is what stops one being mistaken for the other.
        out.append(process.render())

        return out.toString()
    }

    private fun Boolean.toInt(): Int = if (this) 1 else 0

    // Rendered rather than divided into a Double, so a duration never reaches the endpoint in
    // exponential notation — Prometheus accepts it, humans reading a curl do not.
    private fun seconds(millis: Long): String = "%d.%03d".format(millis / 1000, millis % 1000)

    companion object {
        private val MAX_POLL_AGE: Duration = Duration.ofSeconds(30)
        private val COHERENCE_GRACE: Duration = Duration.ofSeconds(30)

        /**
         * Three reconciliation passes. Measured in passes rather than seconds because a pass is
         * what samples a view: anything shorter than [ReconciliationChecker.INTERVAL] would fail
         * the probe on a single view that happened to be mid-flight, which is the ordinary case
         * this window exists to tolerate. Fills arrive at the live site's human rate and a view
         * realigns within a second, so three consecutive passes unable to judge one is not a race
         * — it is a view that has stopped tracking the ledger.
         */
        private val INCONCLUSIVE_GRACE: Duration = ReconciliationChecker.INTERVAL.multipliedBy(3)
    }
}

/*
 * The /readyz body. Property order is the order the uptime monitor and operators have always read;
 * no property has a default, so an absent value is written as an explicit null, as it was before.
 * Properties are private: these classes are only ever written, and the serializer reads the fields.
 */
@Serializable
private class ProbeBody(
    private val ready: Boolean,
    private val consumers: Map<String, ConsumerBody>,
    private val database: DatabaseBody,
    private val views: ViewsBody,
    private val ledger: LedgerBody,
    private val deadLetters: DeadLettersBody,
)

@Serializable
private class ConsumerBody(
    private val ok: Boolean,
    private val threadAlive: Boolean,
    private val assigned: Boolean,
    private val pollAgeMillis: Long?,
    private val fatal: String?,
)

@Serializable
private class DatabaseBody(
    private val ok: Boolean,
)

@Serializable
private class ViewsBody(
    private val ok: Boolean,
    private val positionsOffset: Long?,
    private val exposureOffset: Long?,
    private val coherent: Boolean,
    private val incoherentForMillis: Long?,
)

@Serializable
private class LedgerBody(
    private val ok: Boolean,
    private val checkedAgeMillis: Long?,
    private val views: Map<String, ViewLedgerBody>,
)

@Serializable
private class ViewLedgerBody(
    private val ok: Boolean,
    private val symbolsChecked: Int,
    private val divergences: List<DivergenceBody>,
    private val inconclusive: InconclusiveBody?,
)

@Serializable
private class DivergenceBody(
    private val symbol: String,
    private val ledgerQuantity: Long,
    private val viewQuantity: Long,
    private val difference: Long,
)

@Serializable
private class InconclusiveBody(
    private val viewOffset: Long?,
    private val ledgerOffset: Long?,
    private val forMillis: Long?,
)

@Serializable
private class DeadLettersBody(
    private val published: Long,
    private val failed: Long,
)
