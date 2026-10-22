package com.damianhoward.positionledger.consume

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.math.BigDecimal

/**
 * The shape every orderbook egress record has: one flat JSON object of string and integer values,
 * no nesting, no arrays. Strict — anything outside that shape, a missing field, or a
 * wrongly-typed field throws [IllegalArgumentException], which the consumer maps to the
 * dead-letter topic rather than a retry (malformed input never heals).
 *
 * The text is read by kotlinx.serialization, so every standard JSON escape is understood: the
 * producer now encodes the same way, and a field carrying a newline must not be dead-lettered.
 * The one rule this does not enforce is a duplicated key, where the parser keeps the last value;
 * the producer is an encoder that cannot write one.
 */
internal class FlatJson private constructor(
    private val fields: Map<String, JsonPrimitive>,
) {
    fun string(name: String): String {
        val value = field(name)
        require(value.isString) { "$name must be a JSON string, got ${value.content}" }
        return value.content
    }

    /** The field as a JSON string, or null when absent — for additive optional fields. */
    fun stringOrNull(name: String): String? = if (name in fields) string(name) else null

    fun long(name: String): Long {
        val value = field(name)
        require(!value.isString) { "$name must be a JSON number, got a string" }
        return value.content.toLongOrNull() ?: throw IllegalArgumentException("$name is not an integer: ${value.content}")
    }

    /** A decimal carried as a JSON string — the egress emits prices this way to keep them exact. */
    fun decimal(name: String): BigDecimal =
        try {
            BigDecimal(string(name))
        } catch (_: NumberFormatException) {
            throw IllegalArgumentException("$name is not a decimal: ${string(name)}")
        }

    private fun field(name: String): JsonPrimitive = fields[name] ?: throw IllegalArgumentException("missing field: $name")

    companion object {
        // Plain integers and decimals only: no exponent, no booleans, no null.
        private val NUMBER = Regex("-?\\d+(\\.\\d+)?")

        /** @throws IllegalArgumentException for anything that is not one flat object of strings and numbers */
        fun parse(json: String): FlatJson {
            // A malformed document throws SerializationException, itself an IllegalArgumentException.
            val root = Json.parseToJsonElement(json) as? JsonObject ?: throw IllegalArgumentException("expected a JSON object")
            return FlatJson(
                root.mapValues { (name, value) ->
                    require(value is JsonPrimitive && (value.isString || NUMBER.matches(value.content))) {
                        "unsupported value for $name: $value"
                    }
                    value
                },
            )
        }
    }
}
