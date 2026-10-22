@file:UseSerializers(PlainDecimalSerializer::class)

package com.damianhoward.positionledger.view

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonUnquotedLiteral
import kotlinx.serialization.json.jsonPrimitive
import java.math.BigDecimal

/*
 * The wire shapes of `/api/state` and the SSE stream (LedgerSnapshot v2). Property order is the
 * byte order the desk has always received. No property has a default, because the encoder omits a
 * property that holds its default, and the contract writes absent values as an explicit null.
 * Properties are private: these classes are only ever written, and the serializer reads the fields.
 */

/**
 * Exact decimals as plain JSON numbers — `101.50`, never `1.015E+2` or a float's nearest — the
 * convention risk-engine's report set and the desk reads. kotlinx has no built-in for this, and
 * writing a pre-formatted literal is what JsonUnquotedLiteral exists for.
 */
@OptIn(ExperimentalSerializationApi::class)
internal object PlainDecimalSerializer : KSerializer<BigDecimal> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("PlainDecimal", PrimitiveKind.DOUBLE)

    override fun serialize(
        encoder: Encoder,
        value: BigDecimal,
    ) = (encoder as JsonEncoder).encodeJsonElement(JsonUnquotedLiteral(value.toPlainString()))

    override fun deserialize(decoder: Decoder): BigDecimal = BigDecimal((decoder as JsonDecoder).decodeJsonElement().jsonPrimitive.content)
}

@Serializable
internal class SnapshotWire(
    private val v: Int,
    private val positions: List<PositionWire>,
    private val book: BookWire?,
    private val exposure: ExposureWire,
    private val sync: SyncWire,
)

@Serializable
internal class PositionWire(
    private val symbol: String,
    private val quantity: Long,
    private val lastPrice: BigDecimal,
    private val lastTimeMillis: Long,
)

@Serializable
internal class ProgressWire(
    private val offset: Long,
    private val fillTs: Long,
)

@Serializable
internal class SyncWire(
    private val positions: ProgressWire?,
    private val exposure: ProgressWire?,
    private val coherent: Boolean,
    private val duplicatesDropped: Long,
    private val deadLetters: Long,
)

@Serializable
internal class BookWire(
    private val valuation: BigDecimal,
    private val grossNotional: BigDecimal,
    private val dayPnl: BigDecimal?,
    private val symbols: List<SymbolRiskWire>,
)

/** [report] is risk-engine's own serialisation, carried as parsed JSON so its numbers keep their text. */
@Serializable
internal class SymbolRiskWire(
    private val symbol: String,
    private val openPrice: BigDecimal?,
    private val report: JsonElement,
)

@Serializable
internal class ExposureWire(
    private val maxPosition: Long,
    private val maxNotional: BigDecimal,
    private val symbols: List<SymbolExposureWire>,
    private val events: List<BreachEventWire>,
    private val malformed: Long,
    private val breaches: Long,
    private val progress: ProgressWire?,
)

@Serializable
internal class SymbolExposureWire(
    private val symbol: String,
    private val netQuantity: Long,
    private val lastPrice: BigDecimal,
    private val notional: BigDecimal,
    private val positionUtilisation: BigDecimal,
    private val notionalUtilisation: BigDecimal,
    private val breached: Boolean,
)

@Serializable
internal class BreachEventWire(
    private val symbol: String,
    private val kind: String,
    private val breached: Boolean,
    private val value: BigDecimal,
    private val limit: BigDecimal,
    private val ts: Long,
)
