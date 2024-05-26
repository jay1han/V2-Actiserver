@file:OptIn(ExperimentalUnsignedTypes::class)

import kotlinx.serialization.KSerializer
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encodeToString
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import java.io.File
import java.io.FileNotFoundException
import java.io.FileWriter
import java.io.PrintWriter
import java.nio.file.Path
import java.time.*
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.io.path.Path
import kotlin.io.path.forEachDirectoryEntry
import kotlin.io.path.name

const val VERSION_STRING = "420"

var CENTRAL_HOST = "actimetre.u-paris-sciences.fr"
var USE_HTTPS = true
const val ACTI_PORT = 2883
const val SIDE_PORT = 2882
var MAX_REPO_SIZE = 1_000_000_000L
var MAX_REPO_TIME: Duration = Duration.ofHours(24)
var SYNC_MINS: Duration = Duration.ofMinutes(5)
var CLEANUP_EXEC = ""
var SYNC_EXEC = ""
var INCLUDE_GZ = false
var OUTPUT_RAW = true
var OUTPUT_VECTORS = false
var SECRET_KEY: String = "YouDontKnowThis"

var REPO_ROOT = "/media/actimetre"
const val LOG_FILE = "/etc/actimetre/server.log"
const val REPORT_FILE = "/etc/actimetre/report.log"
const val REGISTRY_FILE = "/etc/actimetre/registry.data"
const val PROJECTS_FILE = "/etc/actimetre/projects.data"
const val CENTRAL_BIN = "/bin/acticentral.py?"
val ACTIM_DEAD_TIME:  Duration = Duration.ofSeconds(3)
val ACTIM_BOOT_TIME:  Duration = Duration.ofSeconds(5)
const val ACTIS_CHECK_SECS = 15L
const val ACTIS_STAT_SECS  = 60L
var LOG_SIZE = 10_000_000
var VERBOSITY = 10
var htmlInfo = ""
var options = Options("")

fun String.runCommand(): String {
    return try {
        val parts = this.split(" ")
        val processBuilder = ProcessBuilder(*parts.toTypedArray())
        processBuilder.redirectErrorStream(true)
        val process = processBuilder.start()
        while (!process.waitFor(1, TimeUnit.SECONDS)) ;
        process.inputStream.bufferedReader().readText().trim()
    } catch(e: Throwable) {
        ""
    }
}

var myMachine: String = ""
var myChannel = 0
var serverId = 0
var serverName = ""
var serverAddress = ""
var myIp = ""
var myIfname = ""
var netConfigOK = ""

class Options(configFileName: String = "") {
    var test: Boolean = false

    init {
        val configFile = File(
            if (configFileName != "") configFileName
            else "/etc/actimetre/actiserver.conf"
        )
        println("Loading options from '${configFile.name}'")
        try {
            configFile.forEachLine {
                if (it.trim() != "" && it[0] != '#') {
                    val (key, value) = it.split("=").map { it.trim() }
                    println("$key = $value")
                    when (key.lowercase()) {
                        "repo_root" -> REPO_ROOT = value
                        "central_host" -> CENTRAL_HOST = value
                        "use_https" -> USE_HTTPS = value.toBoolean()
                        "max_repo_size" -> MAX_REPO_SIZE = value.replace("_", "").toLong()
                        "max_repo_time" -> MAX_REPO_TIME = Duration.ofHours(value.toLong())
                        "secret_key" -> SECRET_KEY = value
                        "cleanup_exec" -> CLEANUP_EXEC = value
                        "sync_exec" -> SYNC_EXEC = value
                        "sync_mins" -> SYNC_MINS = Duration.ofMinutes(value.toLong())
                        "include_gz" -> INCLUDE_GZ = value.toBoolean()
                        "output_vectors" -> OUTPUT_VECTORS = value.toBoolean()
                        "output_raw" -> OUTPUT_RAW = value.toBoolean()
                        "log_size" -> LOG_SIZE = value.replace("_", "").toInt()
                        "verbosity" -> VERBOSITY = value.toInt()
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
            printLog("Config:$e", 1)
        }

        htmlInfo =
            """
            File max size ${MAX_REPO_SIZE.printSize()}, max duration ${MAX_REPO_TIME.print()}<br>
            Sync command: "$SYNC_EXEC", runs ${SYNC_MINS.print()} after death<br>
            Log verbosity $VERBOSITY, max size $LOG_SIZE<br>
            """.trimIndent()
    }
}

fun Init() {
    myMachine = run {
        val inxi = "/usr/bin/inxi -M -c 0".runCommand()
        val regex = "System:\\s+([^:]+)".toRegex()
        val machine = regex.find(inxi)
        if (machine != null) {
            val words = machine.groupValues[1].split(" ")
            words.subList(0, words.size - 1).joinToString(separator = " ")
        } else {
            "Unknown"
        }
    }

    fun getInet(ifname: String): String {
        val config = "/usr/sbin/ifconfig $ifname".runCommand()
        val regex = "inet\\s+([0-9.]+)".toRegex()
        val ipMatch = regex.find(config)
        if (ipMatch != null) return ipMatch.groupValues[1]
        else return ""
    }

    fun findChannel(filename: String): Int? {
        try {
            return "channel=([0-9]+)".toRegex()
                .find(File(filename).readText())?.groupValues?.get(1)?.toInt()
        } catch (e: Throwable) {
            return null
        }
    }

    for (net in "/usr/sbin/ifconfig -s".runCommand().lines()) {
        if (net == "") continue
        val ifname = net.split("\\s".toRegex())[0]
        when (ifname[0]) {
            'e' -> {
                myIp = getInet(ifname)
            }

            'w' -> {
                val iw = "/usr/sbin/iw $ifname info".runCommand()
                if ("type AP".toRegex().find(iw) != null) {
                    myIfname = ifname
                    serverId = "Actis([0-9]{3})".toRegex().find(iw)?.groupValues?.get(1)?.toInt() ?: 0
                    if (serverId > 0) serverName = "Actis%03d".format(serverId)
                    serverAddress = "inet ([0-9.]+)".toRegex()
                        .find("/usr/sbin/ifconfig $ifname".runCommand())?.groupValues?.get(1)
                        ?: "?"
                    myChannel = "channel\\s+([0-9])+".toRegex().find(iw)?.groupValues?.get(1)?.toInt()
                        ?: "Current Frequency:.+Channel\\s+([0-9]+)".toRegex()
                            .find("/usr/sbin/iwlist $myIfname channel".runCommand())?.groupValues?.get(1)?.toInt()
                                ?: findChannel("/etc/hostapd/hostapd.conf")
                                ?: findChannel("/etc/hostapd.conf")
                                ?: 0
                } else {
                    if (myIp == "") {
                        myIp = getInet(ifname)
                    }
                }
            }
        }
    }

    if (myIfname == "") netConfigOK += "Can't find AP interface\n"
    if (myIp == "") netConfigOK += "Can't find my IP\n"
    if (serverId == 0) netConfigOK += "Can't find my server ID\n"
}

class Disk {
    private val df = "/usr/bin/df -B 1 $REPO_ROOT".runCommand().lines()[1].split("\\s+".toRegex())
    val size = df[1].toLong()
    val free = df[3].toLong()
}

class Stat {
    private val diskDevice = "/usr/bin/df $REPO_ROOT".runCommand().lines()[1].split(Regex("\\s+"))[0].split('/').last()
    private val processBuilder = ProcessBuilder("/usr/bin/iostat", "-sxcd", diskDevice, ACTIS_STAT_SECS.toString())
    private val process = processBuilder.start()
    private val iostat = process.inputStream.bufferedReader()
    var cpuIdle: Float = 0.0f
    var memAvailable: Float = 0.0f
    var diskThroughput: Float = 0.0f
    var diskUtilization: Float = 0.0f

    init {
        read()
    }

    fun read() {
        val memFree = "/usr/bin/free".runCommand()
        printLog(memFree, 100)
        val memStat = memFree.lines()[1].split(Regex("\\s+"))
        memAvailable = 100.0f * memStat.last().toFloat() / memStat[1].toFloat()
        val lines = mutableListOf<String>()
        while (iostat.ready()) {
            val line = iostat.readLine()
            printLog(line, 100)
            lines.add(line)
        }
        if (lines.size < 6) return

        val cpuStat = lines[lines.indexOfLast { it.startsWith("avg-cpu") } + 1]
        cpuIdle = cpuStat.split(Regex("\\s+")).last().toFloat()
        val diskStat = lines.findLast { it.startsWith(diskDevice) } ?: ""
        val diskInfoList = diskStat.split(Regex("\\s+"))
        diskThroughput = diskInfoList.getOrNull(2)?.toFloat() ?: 0.0f
        diskUtilization = diskInfoList.lastOrNull()?.toFloat() ?: 0.0f
    }
}

fun globalStat() {
    var disk = Disk()
    while (disk.free < disk.size / 20) {
        var oldestTime = now()
        var oldestFile = ""
        Path(REPO_ROOT).forEachDirectoryEntry {
            val project = it.name
            it.forEachDirectoryEntry {
                val thisRepoFile = it.name
                if ("Actim[0-9]{4}-[12][AB]_[0-9]{14,17}\\.csv".toRegex().matches(thisRepoFile)) {
                    val thisRepoDate = thisRepoFile.parseFileDate()
                    if (thisRepoDate < oldestTime) {
                        oldestFile = "$project/$thisRepoFile"
                        oldestTime = thisRepoDate
                    }
                }
            }
        }
        printLog("Disk full, deleting $oldestFile", 1)
        if (oldestFile != "") {
            Path("$REPO_ROOT/$oldestFile").toFile().delete()
            disk = Disk()
        } else {
            break
        }
    }

    if (disk.free < disk.size / 10 && CLEANUP_EXEC != "") {
        val result = CLEANUP_EXEC.runCommand()
        printLog("CLEAN: \"$CLEANUP_EXEC\" -> $result", 1)
        disk = Disk()
    }

    synchronized(Self) {
        Self.df(disk.size, disk.free)
        Self.stat.read()
        printLog(
            "Disk size ${Self.diskSize}, free ${Self.diskFree} (%.1f%%)"
                .format(100.0 * Self.diskFree / Self.diskSize),
            100
        )
        printLog(
            "CPU Idle %.1f%%, RAM available %.1f%%, Disk throughput %.1fkB/s, utilization %.1f%%"
                .format(Self.stat.cpuIdle, Self.stat.memAvailable, Self.stat.diskThroughput, Self.stat.diskUtilization),
            100
        )
    }
}

lateinit var Self: Actiserver

fun printLog(message: String, verbosity: Int = 1) {
    if (verbosity > VERBOSITY) return
    val append = File(LOG_FILE).length() < LOG_SIZE
    with (PrintWriter(FileWriter(LOG_FILE, append))) {
        println("[${now().prettyFormat()}] $message")
        close()
    }
}

fun printReport(message: String) {
    val append = File(REPORT_FILE).length() < LOG_SIZE
    with (PrintWriter(FileWriter(REPORT_FILE, append))) {
        println("[${now().prettyFormat()}] $message")
        close()
    }
    printLog("REPORT $message", 1)
}

const val HEADER_LENGTH = 5
const val DATA_LENGTH = 12
const val HEADERV3_LENGTH = 8
const val INIT_LENGTH = 13
const val QUERY_LENGTH = 31

var Registry = mutableMapOf<String, Int>()

fun loadRegistry(registryText: String) {
    Registry = Json.decodeFromString<MutableMap<String, Int>>(registryText)
    printLog(Registry.toString(), 10)
}

fun saveRegistry() {
    File(REGISTRY_FILE).writeText(Json.encodeToString<MutableMap<String, Int>>(Registry))
}

fun readRegistry() {
    printLog("Read registry", 1)
    try {
        Registry = Json.decodeFromString<MutableMap<String, Int>>(File(REGISTRY_FILE).readText())
        printLog(Registry.toString(), 100)
    } catch (e: FileNotFoundException) {
        printLog("No local Registry file", 1)
    }
}

var Projects = mutableMapOf<Int, Int>()

fun loadProjects(data: String) {
    for (line in data.lines()) {
        if (line.contains(':')) {
            val project = line.split(':')
            val projectId = project[0].trim().toInt()
            if (project[1].trim() != "") {
                for (actimId in project[1].split(',').map { it.trim().toInt() }) {
                    Projects[actimId] = projectId
                    Self.actimetreList[actimId]?.setProject(projectId)
                }
            }
        }
    }
    printLog(Projects.toString(), 10)
}

fun saveProjects() {
    File(PROJECTS_FILE).writeText(Json.encodeToString<MutableMap<Int, Int>>(Projects))
}

fun readProjects() {
    printLog("Read projects", 1)
    try {
        Projects = Json.decodeFromString<MutableMap<Int, Int>>(File(PROJECTS_FILE).readText())
        printLog(Projects.toString(), 100)
    } catch (e: FileNotFoundException) {
        printLog("No local Projects file", 1)
    }
}

fun String.toFile(projectDir: Path): File {return Path(projectDir.toString(), this).toFile()}

private val actiFormat  : DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
private val fileFormat  : DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss")
private val prettyFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
private val csvFormat:    DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd,HH:mm:ss")

val TimeZero: ZonedDateTime = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneId.of("Z"))

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

fun ZonedDateTime.microFormat(): String {
    return this.format(prettyFormat) + ".%06d".format(this.nano / 1000)
}

fun ZonedDateTime.csvFormat(): String {
    return this.format(csvFormat)
}

fun Duration.print(): String {
    if (this < Duration.ofSeconds(120)) {
        return "${this.toSeconds()}sec"
    } else if (this < Duration.ofMinutes(120)) {
        return "${this.toMinutes()}min"
    } else if (this < Duration.ofHours(48)) {
        return "${this.toHours()}hr"
    } else if (this < Duration.ofDays(60)) {
        return "${this.toDays()}d"
    } else {
        return "${(this.toDays() / 60).toInt()}mo"
    }
}

fun ZonedDateTime.actiFormat(): String {
    return this.format(actiFormat)
}

fun ZonedDateTime.fileFormat(): String {
    return this.format(fileFormat)
}

fun String.parseActiFormat(): ZonedDateTime {
    return ZonedDateTime.of(LocalDateTime.parse(this, actiFormat), ZoneId.of("Z"))
}

fun String.parseFileFormat(): ZonedDateTime {
    return ZonedDateTime.of(LocalDateTime.parse(this, fileFormat), ZoneId.of("Z"))
}

fun String.parseFileDate(): ZonedDateTime {
    if ("[0-9]{14}".toRegex().matches(this.substring(13,27))) {
        return this.substring(13,27).parseActiFormat()
    } else {
        return this.substring(13,30).parseFileFormat()
    }
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

fun UByteArray.getInt3At(index: Int): Int {
    return (this[index].toInt() shl 16) or
            (this[index + 1].toInt() shl 8) or
            this[index + 2].toInt()
}

fun UByteArray.getInt3At20(index: Int): Int {
    return ((this[index].toInt() and 0x0F) shl 16) or
            (this[index + 1].toInt() shl 8) or
            this[index + 2].toInt()
}

fun UByte.parseSensorBits(): String {
    var sensorStr = ""
    for (port in 0..1) {
        var portStr = "%d".format(port + 1)
        for (address in 0..1) {
            val bitMask = 1 shl (port * 4 + address)
            val sensorType = 1 shl (port * 4 + address + 2)
            if ((this.toInt() and bitMask) != 0) {
                if ((this.toInt() and sensorType) != 0)
                    portStr += "%c".format('a' + address)
                else
                    portStr += "%c".format('A' + address)
            }
        }
        if (portStr.length > 1) {
            sensorStr += portStr
        }
    }
    return sensorStr
}

fun UByteArray.dump(): String {
    var dumpString = ""
    for (byte in this) {
        dumpString += "%02X ".format(byte.toInt())
    }
    return dumpString
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

fun runSync(filename: String, block: Boolean = false) {
    if (SYNC_EXEC == "") {
        printLog("SYNC_EXEC empty", 100)
    } else {
        val sync = thread(name = "SYNC($filename)", isDaemon = false, priority = 1) {
            val execString = SYNC_EXEC.replace("$", filename)
            val result = execString.runCommand()
            printLog("SYNC: \"$execString\" -> $result", 10)
        }
        if (block) sync.join()
    }
}
