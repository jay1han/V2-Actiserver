
import java.io.*
import java.time.Duration
import java.util.concurrent.TimeUnit

const val VERSION_STRING = "196"

var CENTRAL_HOST = "localhost"
var HTTP_PORT = 80
var MQTT_HOST = "localhost"
var MQTT_PORT = 1883
var ACTI_PORT = 2883
var MAX_REPO_SIZE = 1_000_000_000
var MAX_REPO_TIME: Duration = Duration.ofHours(24)

const val MQTT_LOG = "Acti/Log"
const val MQTT_TEXT = "Acti"
var REPO_ROOT = "/media/actimetre"
const val LOG_FILE = "/etc/actimetre/server.log"
const val CENTRAL_BIN = "/bin/acticentral.py?"
val ACTIM_REPORT_TIME: Duration = Duration.ofSeconds(5)
val ACTIM_DEAD_TIME: Duration = Duration.ofSeconds(2)
const val ACTIS_CHECK_MILLIS = 15000L

var options = Options("")

class Options(configFileName: String = "") {
    var logging: Boolean = false
    var test: Boolean = false
    var echo: Boolean = false
    var fullText: Boolean = false

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
                        "central_host" -> {
                            CENTRAL_HOST = value
                            MQTT_HOST = value
                        }
                        "mqtt_host" -> MQTT_HOST = value
                        "repo_root" -> REPO_ROOT = value
                        "max_repo_size" -> MAX_REPO_SIZE = value.replace("_", "").toInt()
                        "max_repo_time" -> MAX_REPO_TIME = Duration.ofHours(value.toLong())
                        "options" -> for (c in value.toCharArray()) {
                            when (c) {
                                'l' -> logging = true
                                't' -> test = true
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

val ifconfig = "/usr/sbin/ifconfig -s".runCommand()

val wlan:String = run {
    val regex = "\\n(w\\S+)".toRegex()
    val ifMatch = regex.find(ifconfig)
    if (ifMatch != null) ifMatch.groupValues[1]
    else ""
}
val iw_wlan = "/usr/sbin/iw dev $wlan info".runCommand()

val eth:String = run {
    val regex = "\\n(e\\S+)".toRegex()
    val ifMatch = regex.find(ifconfig)
    if (ifMatch != null) ifMatch.groupValues[1]
    else ""
}

val myChannel: Int = "channel\\s+([0-9])+".toRegex().find(iw_wlan)?.groupValues?.get(1)?.toInt() ?: 0
val serverId: Int = "Actis([0-9]{3})".toRegex().find(iw_wlan)?.groupValues?.get(1)?.toInt() ?: 0
val serverName = "Actis%03d".format(serverId)

val serverAddress: String = run {
    if (wlan == "") "192.168.4.1"
    val config = "/usr/sbin/ifconfig $wlan".runCommand()
    val regex = "inet\\s+([0-9.]+)".toRegex()
    val ipMatch = regex.find(config)
    if (ipMatch != null) ipMatch.groupValues[1]
    else "192.168.4.1"
}

val myIp: String = run {
    if (eth == "") ""
    val config = "/usr/sbin/ifconfig $eth".runCommand()
    val regex = "inet\\s+([0-9.]+)".toRegex()
    val ipMatch = regex.find(config)
    if (ipMatch != null) ipMatch.groupValues[1]
    else ""
}

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
const val INIT_LENGTH = 13
