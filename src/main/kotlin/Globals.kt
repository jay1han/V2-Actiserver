
import java.io.*
import java.time.Duration
import java.util.concurrent.TimeUnit

const val VERSION_STRING = "192"

var CENTRAL_HOST = "192.168.1.9"
var MQTT_PORT = 1883
var ACTI_PORT = 2883
var HTTP_PORT = 80
var MAX_REPO_SIZE = 1_000_000_000
var MAX_REPO_TIME: Duration = Duration.ofHours(24)
var serverId: Int = 999
var serverAddress = "192.168.${serverId}.1"
var serverName = "Actis%03d".format(serverId)
var myChannel = 0

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
    var kill: Boolean = false
    var test: Boolean = false
    var daemon: Boolean = false
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
                        "central_host" -> CENTRAL_HOST = value
                        "repo_root" -> REPO_ROOT = value
                        "max_repo_size" -> MAX_REPO_SIZE = value.toInt()
                        "max_repo_time" -> MAX_REPO_TIME = Duration.ofHours(value.toLong())
                        "server_id" -> {
                            serverId = value.toInt()
                            serverAddress = "192.168.${serverId}.1"
                            serverName = "Actis%03d".format(serverId)
                        }

                        "channel" -> myChannel = value.toInt()
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

val myMachine = run {
    val inxi = "/usr/bin/inxi -M -c 0".runCommand()
    val regex = "System:(\\s[^\\s:]+)+".toRegex()
    val machine = regex.find(inxi)
    if (machine != null) {
        val words = machine.groups.size
        machine.groupValues.subList(1, words).joinToString(separator=" ")
    } else {
        "Unknown"
    }
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
