@file:OptIn(ExperimentalSerializationApi::class, ExperimentalUnsignedTypes::class)

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.MissingFieldException
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.*
import java.time.Duration
import java.util.concurrent.TimeUnit

var CENTRAL_HOST = "localhost"
var UPLOAD_SIZE = 1_000_000
var MAX_REPO_SIZE = 1_000_000_000
var UPLOAD_TIME: Duration = Duration.ofHours(1)
var MAX_REPO_TIME: Duration = Duration.ofHours(24)

const val MQTT_LOG = "Acti/Log"
const val DATA_ROOT = "/media/actimetre/Data"
const val REPO_ROOT = "/media/actimetre/Repo"
const val CONFIG_SELF = "/etc/actimetre/self.data"
const val LOG_FILE = "/etc/actimetre/server.log"
const val CENTRAL_BIN = "/bin/acticentral?"
val ACTIM_REPORT_TIME: Duration = Duration.ofSeconds(15)
val ACTIM_DEAD_TIME: Duration = Duration.ofSeconds(3)
const val ACTIS_CHECK_MILLIS = 15000L

var options = Options("")

class Options(configFileName: String = "") {
    var logging: Boolean = false
    var kill: Boolean = false
    var test: Boolean = false
    var daemon: Boolean = false
    var echo: Boolean = false

    init {
        println("Loading options from '$configFileName'")
        val configFile = File(
            if (configFileName != "") configFileName
            else "/etc/actimetre/actimetre.conf"
        )
        try {
            configFile.forEachLine {
                val (key, value) = it.split("=").map { it.trim() }
                when (key.lowercase()) {
                    "central_host" -> CENTRAL_HOST = value
                    "upload_size" -> UPLOAD_SIZE = value.toInt()
                    "max_repo_size" -> MAX_REPO_SIZE = value.toInt()
                    "upload_time" -> UPLOAD_TIME = Duration.ofHours(value.toLong())
                    "max_repo_time" -> MAX_REPO_TIME = Duration.ofHours(value.toLong())
                    "options" -> parseOptions(value)
                }
            }
        } catch (e: FileNotFoundException) {}
    }

    fun parseOptions(options: String) {
        for (c in options.toCharArray()) {
            when (c) {
                'l' -> logging = true
                'k' -> kill = true
                't' -> test = true
                'd' -> daemon = true
                'e' -> echo = true
                else -> {}
            }
        }
    }
}

fun String.runCommand(): String {
    return try {
        val parts = this.split(" ")
        val process = ProcessBuilder(*parts.toTypedArray()).start()
        process.waitFor(5, TimeUnit.SECONDS)
        process.inputStream.bufferedReader().readText()
    } catch(e: IOException) {
        ""
    }
}

val myMac = run {
    val ifconfig = "/usr/sbin/ifconfig wlan0".runCommand()
    val regex = "ether\\s+([0-9a-fA-F:]+)".toRegex()
    val mac = regex.find(ifconfig)
    if (mac != null) {
        mac.groups[1]!!.value.uppercase().filterNot { it == ':' }
    } else {
        ""
    }
}

val myIp = run {
    val ifconfig = "/usr/sbin/ifconfig eth0".runCommand()
    val regex = "inet\\s+([0-9.]+)".toRegex()
    val ip = regex.find(ifconfig)
    if (ip != null) {
        ip.groups[1]!!.value
    } else {
        ""
    }
}

val mySsid = BufferedReader(FileReader("/etc/hostapd/hostapd.conf"))
    .readLines().map {it.split("=")}
    .map { it -> it.map {it.trim()}}
    .map {if (it[0] == "ssid") it[1] else ""}
    .find {it != ""}

val serverId: Int = mySsid?.substring(5, 8)?.toInt() ?: 0

var Self = Actiserver()

fun loadSelf() {
    try {
        val selfFile = BufferedReader(FileReader(CONFIG_SELF))
        try {
            Self = Json.decodeFromString<Actiserver>(selfFile.readText())
        } catch (e: MissingFieldException) {
            Self = Actiserver(serverId, myMac, myIp, now())
            dumpSelf()
        }
        selfFile.close()
    } catch(e: FileNotFoundException) {
        Self = Actiserver(serverId, myMac, myIp, now())
        dumpSelf()
    }
}
fun dumpSelf() {
    printLog("dumpSelf\n" + Json.encodeToString(Self))
    val selfFile = BufferedWriter(FileWriter(CONFIG_SELF))
    selfFile.write(Json.encodeToString(Self))
    selfFile.close()
}

fun printLog(message: String) {
    if (options.echo) println(message)
    with (PrintWriter(FileWriter(LOG_FILE, true))) {
        println(message)
        close()
    }
}
