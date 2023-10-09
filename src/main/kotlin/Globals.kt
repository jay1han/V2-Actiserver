@file:OptIn(ExperimentalUnsignedTypes::class)

import kotlinx.serialization.KSerializer
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import java.io.File
import java.io.FileWriter
import java.io.PrintWriter
import java.time.*
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

const val VERSION_STRING = "265"

var CENTRAL_HOST = "actimetre.u-paris-sciences.fr"
var USE_HTTPS = true
var ACTI_PORT = 2883
var MAX_REPO_SIZE = 1_000_000_000
var MAX_REPO_TIME: Duration = Duration.ofHours(24)
var SECRET_KEY: String = "YouDontKnowThis"

var REPO_ROOT = "/media/actimetre"
const val LOG_FILE = "/etc/actimetre/server.log"
const val CENTRAL_BIN = "/bin/acticentral.py?"
val ACTIM_DEAD_TIME:  Duration = Duration.ofSeconds(3)
val ACTIM_BOOT_TIME:  Duration = Duration.ofSeconds(5)
const val ACTIS_CHECK_SECS = 15L
const val LOG_SIZE = 1_000_000

var options = Options("")

class Options(configFileName: String = "") {
    var test: Boolean = false
    var isLocal: Boolean = false

    init {
        println("Loading options from '$configFileName'")
        val configFile = File(
            if (configFileName != "") configFileName
            else "/etc/actimetre/actiserver.conf"
        )
        try {
            configFile.forEachLine {
                if (it.trim() != "" && it[0] != '#') {
                    val (key, value) = it.split("=").map { it.trim() }
                    when (key.lowercase()) {
                        "repo_root" -> REPO_ROOT = value
                        "local_repo" -> isLocal = value.lowercase().toBoolean()
                        "central_host" -> CENTRAL_HOST = value
                        "use_https" -> USE_HTTPS = value.toBoolean()
                        "max_repo_size" -> MAX_REPO_SIZE = value.replace("_", "").toInt()
                        "max_repo_time" -> MAX_REPO_TIME = Duration.ofHours(value.toLong())
                        "secret_key" -> SECRET_KEY = value
                        "options" -> for (c in value.toCharArray()) {
                            when (c) {
                                't' -> test = true
                                else -> {}
                            }
                        }
                    }
                }
            }
        } catch (e: Throwable) {
            printLog("Config:$e")
        }
    }
}

fun String.runCommand(): String {
    return try {
        val parts = this.split(" ")
        val process = ProcessBuilder(*parts.toTypedArray()).start()
        process.waitFor(5, TimeUnit.SECONDS)
        process.inputStream.bufferedReader().readText()
    } catch(e: Throwable) {
        ""
    }
}

val myMachine = run {
    val inxi = "/usr/bin/inxi -M -c 0".runCommand()
    val regex = "System:\\s+([^:]+)".toRegex()
    val machine = regex.find(inxi)
    if (machine != null) {
        val words = machine.groupValues[1].split(" ")
        words.subList(0, words.size - 1).joinToString(separator=" ")
    } else {
        "Unknown"
    }
}

val ifconfig = "/usr/sbin/ifconfig -a".runCommand()

val wlan:String = run {
    val regex = "^(w[^:]+).+RUNNING".toRegex(RegexOption.MULTILINE)
    val ifMatch = regex.find(ifconfig)
    if (ifMatch != null) ifMatch.groupValues[1]
    else ""
}
val iw_wlan = "/usr/sbin/iw dev $wlan info".runCommand()

val eth:String = run {
    val regex = "^(e[^:]+)".toRegex(RegexOption.MULTILINE)
    val ifMatch = regex.find(ifconfig)
    if (ifMatch != null) ifMatch.groupValues[1]
    else ""
}

fun findChannel(filename: String): Int? {
    try {
        return "channel=([0-9]+)".toRegex()
            .find(File(filename).readText())?.groupValues?.get(1)?.toInt()
    } catch(e:Throwable) {return null}
}

val myChannel: Int =
    "channel\\s+([0-9])+".toRegex().find(iw_wlan)?.groupValues?.get(1)?.toInt()
        ?: "Current Frequency:.+Channel\\s+([0-9]+)".toRegex()
            .find("/usr/sbin/iwlist $wlan channel".runCommand())?.groupValues?.get(1)?.toInt()
        ?: findChannel("/etc/hostapd/hostapd.conf")
        ?: findChannel("/etc/hostapd.conf")
        ?: 0

val serverId: Int = "Actis([0-9]{3})".toRegex().find(iw_wlan)?.groupValues?.get(1)?.toInt() ?: 0
val serverName = "Actis%03d".format(serverId)

val serverAddress: String = run {
    if (wlan == "") "192.168.4.1"
    else {
        val config = "/usr/sbin/ifconfig $wlan".runCommand()
        val regex = "inet\\s+([0-9.]+)".toRegex()
        val ipMatch = regex.find(config)
        if (ipMatch != null) ipMatch.groupValues[1]
        else "192.168.4.1"
    }
}

val myIp: String = run {
    if (eth == "") ""
    else {
        val config = "/usr/sbin/ifconfig $eth".runCommand()
        val regex = "inet\\s+([0-9.]+)".toRegex()
        val ipMatch = regex.find(config)
        if (ipMatch != null) ipMatch.groupValues[1]
        else ""
    }
}

val localRepo: Boolean = run {
    val df = "/usr/bin/df $REPO_ROOT".runCommand().lines()[1]
    df.startsWith("/dev/")
}

fun diskCapa() {
    val df = "/usr/bin/df -B 1 $REPO_ROOT".runCommand().lines()[1].split("\\s+".toRegex())
    val size = df[1].toLong()
    val free = df[3].toLong()
    Self.df(size, free)
}

lateinit var Self: Actiserver

fun printLog(message: String) {
    val append = File(LOG_FILE).length() < LOG_SIZE
    with (PrintWriter(FileWriter(LOG_FILE, append))) {
        println("[${now().prettyFormat()}] $message")
        close()
    }
}

const val HEADER_LENGTH = 5
const val DATA_LENGTH = 12
const val INIT_LENGTH = 13

var Registry = mutableMapOf<String, Int>()

fun loadRegistry(registryText: String) {
    Registry = Json.decodeFromString<MutableMap<String, Int>>(registryText)
}

fun String.fullName(): String {return "$REPO_ROOT/$this"}

private val actiFormat  : DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
private val prettyFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
private val csvFormat:    DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd,HH:mm:ss")

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

fun ZonedDateTime.csvFormat(): String {
    return this.format(csvFormat)
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

fun Long.printSize(): String {
    if (this == 0L) return "0"
    var unit = 1_000_000L
    var unitStr = "MB"
    var precision = 2
    if (this > 1_000_000_000L) {
        unit = 1_000_000_000L
        unitStr = "GB"
        if (this < 10_000_000_000L) precision = 1
    } else {
        if (this >= 100_000_000L) precision = 0
        else if (this >= 10_000_000L) precision = 1
        else precision = 2
    }
    val inUnits = this.toDouble() / unit.toDouble()
    return "%.${precision}f$unitStr".format(inUnits)
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