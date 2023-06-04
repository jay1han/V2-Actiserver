
import java.io.*
import java.time.Duration
import java.util.concurrent.TimeUnit

const val VERSION_STRING = "231"

var CENTRAL_HOST = "actimetre.fr"
var HTTP_PORT = 80
var ACTI_PORT = 2883
var MAX_REPO_SIZE = 1_000_000_000
var MAX_REPO_TIME: Duration = Duration.ofHours(24)

var REPO_ROOT = "/media/actimetre"
const val LOG_FILE = "/etc/actimetre/server.log"
const val CENTRAL_BIN = "/bin/acticentral.py?"
val ACTIM_DEAD_TIME:  Duration = Duration.ofSeconds(3)
val ACTIM_BOOT_TIME:  Duration = Duration.ofSeconds(5)
const val ACTIS_CHECK_SECS = 15L
const val LOG_SIZE = 1_000_000

var options = Options("")

class Options(configFileName: String = "") {
    var logging: Boolean = true
    var test: Boolean = false
    var echo: Boolean = false

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
                        "central_host" -> CENTRAL_HOST = value
                        "max_repo_size" -> MAX_REPO_SIZE = value.replace("_", "").toInt()
                        "max_repo_time" -> MAX_REPO_TIME = Duration.ofHours(value.toLong())
                        "options" -> for (c in value.toCharArray()) {
                            when (c) {
                                'l' -> logging = false
                                't' -> test = true
                                'e' -> echo = true
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

val myChannel: Int = "channel\\s+([0-9])+".toRegex().find(iw_wlan)?.groupValues?.get(1)?.toInt() ?:
        "Current Frequency:.+Channel\\s+([0-9]+)".toRegex()
            .find("/usr/sbin/iwlist $wlan channel".runCommand())?.groupValues?.get(1)?.toInt() ?: 0
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

lateinit var Self: Actiserver

fun printLog(message: String) {
    if (options.echo) println(message)
    val append = File(LOG_FILE).length() < LOG_SIZE
    with (PrintWriter(FileWriter(LOG_FILE, append))) {
        println("[${now().prettyFormat()}] $message")
        close()
    }
}

const val HEADER_LENGTH = 5
const val DATA_LENGTH = 12
const val INIT_LENGTH = 13
