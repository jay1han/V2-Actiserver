@file:OptIn(ExperimentalSerializationApi::class)

import kotlinx.serialization.ExperimentalSerializationApi
import java.io.*
import java.time.Duration
import java.util.concurrent.TimeUnit

var CENTRAL_HOST = "localhost"
var UPLOAD_SIZE = 1_000_000
var MAX_REPO_SIZE = 1_000_000_000
var UPLOAD_TIME: Duration = Duration.ofHours(1)
var MAX_REPO_TIME: Duration = Duration.ofHours(24)

const val MQTT_LOG = "Acti/Log"
const val MQTT_TEXT = "Acti"
const val DATA_ROOT = "/media/actimetre/Data"
const val REPO_ROOT = "/media/actimetre/Repo"
const val LOG_FILE = "/etc/actimetre/server.log"
const val CENTRAL_BIN = "/bin/acticentral.py?"
val ACTIM_REPORT_TIME: Duration = Duration.ofSeconds(5)
val ACTIM_DEAD_TIME: Duration = Duration.ofSeconds(2)
const val ACTIS_CHECK_MILLIS = 10000L

var options = Options("")

class Options(configFileName: String = "") {
    var logging: Boolean = false
    var kill: Boolean = false
    var test: Boolean = false
    var daemon: Boolean = false
    var echo: Boolean = false
    var fullText: Boolean = false

    init {
        println("Loading options from '$configFileName'")
        val configFile = File(
            if (configFileName != "") configFileName
            else "/etc/actimetre/actimetre.conf"
        )
        try {
            configFile.forEachLine {
                if (it.trim() != "") {
                    val (key, value) = it.split("=").map { it.trim() }
                    when (key.lowercase()) {
                        "central_host" -> CENTRAL_HOST = value
                        "upload_size" -> UPLOAD_SIZE = value.toInt()
                        "max_repo_size" -> MAX_REPO_SIZE = value.toInt()
                        "upload_time" -> UPLOAD_TIME = Duration.ofHours(value.toLong())
                        "max_repo_time" -> MAX_REPO_TIME = Duration.ofHours(value.toLong())
                        "options" -> for (c in value.toCharArray()) {
                            when (c) {
                                'l' -> logging = true
                                'k' -> kill = true
                                't' -> test = true
                                'd' -> daemon = true
                                'e' -> echo = true
                                'f' -> fullText = true
                                else -> {}
                            }
                        }
                    }
                }
            }
        } catch (e: FileNotFoundException) {}
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
val serverName = "Actis%03d".format(serverId)

lateinit var Self: Actiserver

fun printLog(message: String) {
    if (options.echo) println(message)
    with (PrintWriter(FileWriter(LOG_FILE, true))) {
        println("[$serverName] $message")
        close()
    }
}

const val HEADER_LENGTH = 5
const val DATA_LENGTH = 12
const val INIT_LENGTH = 10
