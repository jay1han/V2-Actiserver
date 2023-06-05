@file:OptIn(ExperimentalUnsignedTypes::class)

import kotlinx.serialization.KSerializer
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import java.time.*
import java.time.format.DateTimeFormatter

var Registry = mutableMapOf<String, Int>()

fun loadRegistry(registryText: String) {
    Registry = Json.decodeFromString<MutableMap<String, Int>>(registryText)
}

fun String.fullName(): String {return "$REPO_ROOT/$this"}

private val actiFormat  : DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
private val prettyFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
val TimeZero = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneId.of("Z"))

object DateTimeAsString: KSerializer<ZonedDateTime> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("ZonedDateTime", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: ZonedDateTime) {
        encoder.encodeString(value.format(actiFormat))
    }
    override fun deserialize(decoder: Decoder): ZonedDateTime {
        return ZonedDateTime.of(LocalDateTime.parse(decoder.decodeString(), actiFormat), ZoneId.of("Z"))
    }
}

fun now(): ZonedDateTime {
    return ZonedDateTime.now(Clock.systemUTC())
}

fun ZonedDateTime.prettyFormat(): String {
    return this.format(prettyFormat)
}

fun Duration.printSec(): String {
    return "${this.toSeconds().toString()}s"
}

fun ZonedDateTime.actiFormat(): String {
    return this.format(actiFormat)
}

fun String.parseActiFormat(): ZonedDateTime {
    return ZonedDateTime.of(LocalDateTime.parse(this, actiFormat), ZoneId.of("Z"))
}

fun String.parseFileDate(): ZonedDateTime {
    return this.substring(13,27).parseActiFormat()
}

fun UByteArray.getInt3At(index: Int): Long {
    return (this[index].toLong() shl 16) or
            (this[index + 1].toLong() shl 8) or
            this[index + 2].toLong()
}

fun UByte.parseSensorBits(): String {
    var sensorStr = ""
    for (port in 0..1) {
        var portStr = "%d".format(port + 1)
        for (address in 0..1) {
            val bitMask = 1 shl (port * 4 + address)
            if ((this.toInt() and bitMask) != 0) {
                portStr += "%c".format('A' + address)
            }
        }
        if (portStr.length > 1) {
            sensorStr += portStr
        }
    }
    return sensorStr
}

fun String.cleanJson(): String {
    return this
        .replace(" ", "")
        .replace("\\\"", "")
        .replace("\"", "")
        .replace("\\", "")
        .replace("[]", "empty")
        .replace("[", "[\n")
        .replace("},", "},\n")
}