@file:OptIn(ExperimentalUnsignedTypes::class)

import kotlinx.serialization.*
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.nio.ByteBuffer
import java.nio.channels.ByteChannel
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.io.path.Path
import kotlin.io.path.fileSize
import kotlin.io.path.forEachDirectoryEntry

@OptIn(ExperimentalUnsignedTypes::class)
class Record(buffer: UByteArray, val sensorId: String, bootEpoch: Long, msgBootEpoch: Long, msgMillis: Int) {
    private val diffMillis = buffer[0].toInt() * 256 + buffer[1].toInt()
    private val adjEpoch = if (msgMillis + diffMillis > 1000) 1 else 0
    val dateTime: ZonedDateTime = ZonedDateTime.ofInstant(
        Instant.ofEpochSecond((msgBootEpoch + bootEpoch + adjEpoch),
            ((msgMillis + diffMillis) % 1000).toLong() * 1_000_000L),
        ZoneId.of("Z"))
    private val accelStr = makeAccelStr(buffer.sliceArray(2..7))
    private val gyroStr = makeGyroStr(buffer.sliceArray(8..11))
    val textStr: String = dateTime.prettyFormat() +
            ".%03d ".format(dateTime.nano / 1000000L) +
            accelStr + " " + gyroStr

    private fun makeInt(msb: UByte, lsb: UByte) : Int {
        var integer = msb.toInt() * 256 + lsb.toInt()
        if (integer >= 32768) integer -= 65536
        return integer
    }

    @OptIn(ExperimentalUnsignedTypes::class)
    private fun makeAccelStr(buffer: UByteArray): String{
        val rawX = makeInt(buffer[0], buffer[1])
        val rawY = makeInt(buffer[2], buffer[3])
        val rawZ = makeInt(buffer[4], buffer[5])
        return arrayOf(
            rawX / 8192.0f,
            rawY / 8192.0f,
            rawZ / 8192.0f
        ).joinToString(separator = " ") { "%+7.4f".format(it) }
    }

    @OptIn(ExperimentalUnsignedTypes::class)
    private fun makeGyroStr(buffer: UByteArray): String {
        val rawX = makeInt(buffer[0], buffer[1])
        val rawY = makeInt(buffer[2], buffer[3])
        return arrayOf(
            rawX / 131.0f,
            rawY / 131.0f
        ).joinToString(separator = " ") { "%+7.3f".format(it) }
    }
}

@Serializable
class SensorInfo(
    @Required private val actimId: Int = 0,
    @Required private val sensorId: String = "",
    @Required var fileName: String = "",
    @Required private var fileSize: Int = 0,
    @Transient private var fileDate: ZonedDateTime = TimeZero
){
    @Transient lateinit var fileHandle: BufferedWriter

    private fun sensorName(): String {return "Actim%04d-%s".format(actimId, sensorId)}

    private fun findDataFile(atDateTime: ZonedDateTime): Boolean {
        diskCapa()

        var lastRepoFile = ""
        var lastRepoSize = 0
        var lastRepoDate = TimeZero
        Path(REPO_ROOT).forEachDirectoryEntry {
            val thisRepoFile = it.fileName.toString()
            if ("Actim[0-9]{4}-[12][AB]_[0-9]{14}\\.txt".toRegex().matches(thisRepoFile)) {
                val thisRepoDate = thisRepoFile.parseFileDate()
                if (sensorName() == thisRepoFile.substring(0, 12)) {
                    if (lastRepoFile == "" ||
                        (Duration.between(lastRepoDate, thisRepoDate) > Duration.ofSeconds(0))
                    ) {
                        lastRepoFile = thisRepoFile
                        lastRepoSize = it.fileSize().toInt()
                        lastRepoDate = thisRepoDate
                    }
                }
            }
        }

        if (lastRepoFile == ""
            || (Duration.between(lastRepoDate, atDateTime) > MAX_REPO_TIME)
            || (lastRepoSize > MAX_REPO_SIZE)) {
            newDataFile(atDateTime)
            return true
        } else {
            fileName = lastRepoFile
            fileDate = lastRepoDate
            fileSize = lastRepoSize
            val file = File(lastRepoFile.fullName())
            file.setWritable(true, false)
            fileHandle = BufferedWriter(FileWriter(file, true), 16384)
            printLog("Continue data file $lastRepoFile")
        }
        return false
    }

    private fun newDataFile(atDateTime: ZonedDateTime) {
        fileName = sensorName() + "_" + atDateTime.actiFormat() + ".txt"
        fileDate = atDateTime
        fileSize = 0
        val file = File(fileName.fullName())
        file.setWritable(true, false)
        fileHandle = BufferedWriter(FileWriter(file), 16384)
        printLog("Start data file $fileName")
    }

    fun writeData(record: Record): Pair<Boolean, Int> {
        var newFile: Boolean = false
        if (!this::fileHandle.isInitialized) newFile = findDataFile(record.dateTime)
        else if (fileSize > MAX_REPO_SIZE ||
            Duration.between(fileDate, record.dateTime) > MAX_REPO_TIME
        ) {
            fileHandle.close()
            diskCapa()
            newDataFile(record.dateTime)
            newFile = true
        }
        fileHandle.append(record.textStr + "\n")
        fileSize += record.textStr.length + 1
        return Pair(newFile, record.textStr.length + 1)
    }

    fun closeIfOpen() {
        if (this::fileHandle.isInitialized) {
            try {
                fileHandle.close()
            } catch (e: Throwable) {
                printLog("Close file:$e")
            }
            diskCapa()
        }
    }
}

val Frequencies = listOf(50, 100, 1, 200, 30, 10)
const val FREQ_COUNT = 6

@Serializable
class ActimetreShort(
    @Required var actimId           : Int = 9999,
    @Required private var mac       : String = "............",
    @Required private var boardType : String = "???",
    @Required private var version   : String = "000",
    @Required private var serverId  : Int = 0,
    @Required private var isDead    : Boolean = false,
    @Serializable(with = DateTimeAsString::class)
    @Required var bootTime          : ZonedDateTime = TimeZero,
    @Serializable(with = DateTimeAsString::class)
    @Required var lastSeen          : ZonedDateTime = TimeZero,
    @Serializable(with = DateTimeAsString::class)
    @Required var lastReport        : ZonedDateTime = TimeZero,
    @Required var sensorStr         : String = "",
    @Required var frequency         : Int = 0,
    @Required var rating            : Double = 1.0,
    @Required var rssi              : Int = 0,
    @Required var repoNums          : Int = 0,
    @Required var repoSize          : Long = 0,
) {
    fun init(a: Actimetre): ActimetreShort {
        actimId = a.actimId
        mac = a.mac
        boardType = a.boardType
        version = a.version
        serverId = a.serverId
        isDead = a.isDead
        bootTime = a.bootTime
        lastSeen = a.lastSeen
        lastReport = a.lastReport
        sensorStr = a.sensorStr()
        frequency = a.frequency
        rating = a.rating
        rssi = a.rssi
        repoNums = a.repoNums
        repoSize = a.repoSize
        return this
    }
}

class Actimetre(
    val actimId   : Int = 0,
    var mac       : String = "............",
    var boardType : String = "???",
    var version   : String = "000",
    val serverId  : Int = 0,
) {
    var isDead = false
    var bootTime: ZonedDateTime = TimeZero
    var lastSeen: ZonedDateTime = TimeZero
    var lastReport: ZonedDateTime = TimeZero
    private var sensorList = mutableMapOf<String, SensorInfo>()
    private var nSensors = 0
    private lateinit var channel: ByteChannel
    private var msgLength = 0
    private var sensorOrder = mutableListOf<String>()
    private var bootEpoch = 0L
    var frequency = 50
    private var cycleNanoseconds = 1_000_000_000L / frequency
    private var lastMessage = TimeZero
    private var missingPoints  = 0
    private var totalPoints = 0
    var rating = 0.0
    var rssi: Int = 0
    var repoNums: Int = 0
    var repoSize: Long = 0

    @OptIn(ExperimentalUnsignedTypes::class)
    fun run(channel: ByteChannel) {
        this.channel = channel
        while (true) {
            var inputLen = 0
            val sensorBuffer = ByteBuffer.allocate(msgLength)
            val timeout = now().plusSeconds(1)
            try {
                while (inputLen < msgLength && now().isBefore(timeout)) {
                    inputLen += this.channel.read(sensorBuffer)
                }
            } catch (e: Throwable) {
                printLog("${actimName()}:$e")
                return
            }
            if (inputLen != msgLength) {
                printLog("${actimName()} sent $inputLen bytes < $msgLength. Skipping")
            } else {
                lastSeen = now()
                val sensorData = sensorBuffer.array().toUByteArray()
                val msgBootEpoch = sensorData.getInt3At(0)
                val msgMillis = (sensorData[3].toInt() and 0x03) * 256 +
                        sensorData[4].toInt()
                val msgDateTime = ZonedDateTime.ofInstant(
                    Instant.ofEpochSecond(msgBootEpoch + bootEpoch, msgMillis * 1_000_000L),
                    ZoneId.of("Z")
                )

                var msgFrequency = (sensorData[3].toInt() shr 2) and 0x07
                if (msgFrequency >= FREQ_COUNT) {
                    printLog("Unknown frequency code $msgFrequency, revert to base")
                    msgFrequency = 0
                }
                frequency = Frequencies[msgFrequency]
                cycleNanoseconds = 1_000_000_000L / frequency

                rssi = (sensorData[3].toInt() shr 5) and 0x07

                if (lastMessage == TimeZero) {
                    totalPoints = 1
                    missingPoints = 0

                    Path(REPO_ROOT).forEachDirectoryEntry("${actimName()}*") {
                        repoSize += it.fileSize()
                        repoNums ++
                    }
                } else {
                    val cycles = Duration.between(lastMessage, msgDateTime)
                        .dividedBy(Duration.ofNanos(cycleNanoseconds))
                        .toInt()
                    if (cycles > frequency) {
                        printLog("${actimName()} jumped over ${cycles-1} cycles")
                    }
                    totalPoints += cycles
                    if (cycles > 1) {
                        missingPoints += cycles - 1
                        printLog("${actimName()} missed $cycles cycles $missingPoints / $totalPoints = ${missingPoints.toDouble() / totalPoints}")
                    }
                    rating = missingPoints.toDouble() / totalPoints.toDouble()
                }
                lastMessage = msgDateTime.minusNanos(cycleNanoseconds / 10L)

                var index = 5
                while (index < msgLength) {
                    val record = Record(
                        sensorData.sliceArray(index until (index + DATA_LENGTH)),
                        sensorOrder[index / DATA_LENGTH], bootEpoch, msgBootEpoch, msgMillis
                    )
                    if (!sensorList.containsKey(record.sensorId))
                        sensorList[record.sensorId] = SensorInfo(actimId, record.sensorId)
                    val (newFile, sizeWritten) = sensorList[record.sensorId]!!.writeData(record)
                    repoSize += sizeWritten
                    if (newFile) repoNums ++
                    if (newFile or (repoSize % 100_000 < 64)) {
                        htmlData()
                    }
                    index += DATA_LENGTH
                }
            }
        }
    }

    fun htmlData() {
        var repoList: MutableMap<String, MutableList<String>> = mutableMapOf()
        Path(REPO_ROOT).forEachDirectoryEntry("${actimName()}*") {
            val fileDate = it.fileName.toString().parseFileDate().prettyFormat()
            val sensorStr = it.fileName.toString().substring(10,12)
            val fileSize = it.fileSize().printSize()
            if (repoList.get(sensorStr) == null) repoList[sensorStr] = mutableListOf()
            repoList[sensorStr]!!.add("""
            <td>$fileDate</td><td>$fileSize</td>
            <td><a href="/${it.fileName}">${it.fileName}</a></td>                
            """.trimIndent())
        }

        val htmlFile = FileWriter("$REPO_ROOT/index%04d.html".format(actimId))
        htmlFile.write("""
            <html><head>
            <style>
            body {font-family:"Arial", "Helvetica", "Verdana", "Calibri", sans-serif; hyphens:manual;}
            table,th,tr,td {border:1px solid black; padding:0.3em; margin:0; border-collapse:collapse; text-align:center;}
            </style>
            <title>${actimName()} data files</title></head><body>
            <h1>${actimName()} data files</h1>
            <p>Files are locally stored on ${Self.serverName()}, IP=${Self.ip}</p>
            <p>Right-click a file name and choose "Download link" to retrieve the file</p>
            <table><tr><th>Sensor</th><th>Date created</th><th>Size</th><th>File name</th></tr>
        """.trimIndent())
        for (sensor in repoList.keys.sorted()) {
            val lines = repoList[sensor]!!.sorted()
            htmlFile.write("<tr><td rowspan=${lines.size}>$sensor</td>\n")
            htmlFile.write("${lines[0]}</tr>\n")
            for (line in lines.slice(1..lines.size - 1)) {
                htmlFile.write("<tr>$line</tr>\n")
            }
        }
        htmlFile.write("""
            </table></body></html>
        """.trimIndent())
        htmlFile.close()
    }

    fun dies() {
        if (isDead) return
        isDead = true
        frequency = 0
        val reqString = CENTRAL_BIN + "action=actimetre-off" +
                "&serverId=${serverId}&actimId=${actimId}"
        sendHttpRequest(reqString)
        printLog("${actimName()} dies")
        for (sensorInfo in sensorList.values) {
            sensorInfo.closeIfOpen()
        }
        synchronized(Self) {
            Self.removeActim(actimId)
        }
        if (this::channel.isInitialized) channel.close()
    }

    fun sensorStr(): String {
        var result = ""
        for (port in IntRange(1, 2)) {
            var portStr = "$port"
            for (address in 'A'..'B') {
                if (sensorList.containsKey("$port$address")) {
                    portStr += "$address"
                }
            }
            if (portStr.length > 1) result += portStr
        }
        return result
    }

    fun loop(now: ZonedDateTime) {
        if (lastSeen == TimeZero || isDead || !this::channel.isInitialized) return
        if (Duration.between(bootTime, now) < ACTIM_BOOT_TIME) return
        if (Duration.between(lastSeen, now) > ACTIM_DEAD_TIME) {
            if (isDead) return
            printLog(
                "${actimName()} last seen ${lastSeen.prettyFormat()}, " +
                        "${Duration.between(lastSeen, now).printSec()} before now ${now.prettyFormat()}"
            )
            if (this::channel.isInitialized) channel.close()
            dies()
        }
    }

    fun setInfo(mac: String, boardType: String, version: String, bootTime: ZonedDateTime, sensorBits: UByte) {
        this.mac = mac
        this.boardType = boardType
        this.version = version
        this.bootTime = bootTime
        lastSeen = bootTime
        lastReport = TimeZero
        totalPoints = 0
        missingPoints = 0
        rating = 0.0
        bootEpoch = bootTime.toEpochSecond()
        nSensors = 0
        for (port in 0..1) {
            for (address in 0..1) {
                val bitMask = 1 shl (port * 4 + address)
                val sensorId = "%d%c".format(port + 1, 'A' + address)
                    if ((sensorBits.toInt() and bitMask) != 0) {
                        sensorList[sensorId] = SensorInfo(actimId, sensorId)
                        nSensors += 1
                        sensorOrder.add(sensorId)
                }
            }
        }
        msgLength = nSensors * DATA_LENGTH + HEADER_LENGTH
        sensorOrder.sort()
    }

    fun actimName(): String {
        return "Actim%04d".format(actimId)
    }
}

object ActimetreShortList: KSerializer<Map<Int, ActimetreShort>> {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("Map<Int, ActimetreShort>", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: Map<Int, ActimetreShort>) {
        val stringList: MutableList<String> = mutableListOf()
        for (a in value.values) {
            stringList += Json.encodeToString(a)
        }
        encoder.encodeString(stringList.joinToString(separator = ",", prefix = "[", postfix = "]"))
    }
    override fun deserialize(decoder: Decoder): Map<Int, ActimetreShort> {
        return mapOf()
    }
}

@Serializable
class ActiserverShort(
    @Required var serverId: Int = 0,
    @Required var machine : String = "Unknown",
    @Required var version : String = "000",
    @Required var channel : Int = 0,
    @Required var ip: String = "0.0.0.0",
    @Required var isLocal: Boolean = false,
    @Required var diskSize: Long = 0,
    @Required var diskFree: Long = 0,
    @Serializable(with = DateTimeAsString::class)
    @Required var lastReport : ZonedDateTime = TimeZero,
    @Serializable(with = ActimetreShortList::class)
    @Required var actimetreList: Map<Int, ActimetreShort> = mapOf(),
) {
    fun init(s: Actiserver) : ActiserverShort {
        serverId = s.serverId
        machine = s.machine
        version = s.version
        channel = s.channel
        ip = s.ip
        isLocal = s.isLocal
        diskSize = s.diskSize
        diskFree = s.diskFree
        lastReport = s.lastReport
        for (a in s.actimetreList.values) {
            a.lastReport = s.lastReport
        }
        actimetreList = s.actimetreList.map { Pair(it.key, ActimetreShort().init(it.value)) }.toMap()
        return this
    }
}

class Actiserver(
    val serverId: Int = 0,
    val machine : String = "Unknown",
    val version : String = "000",
    val channel : Int = 0,
    val ip      : String = "0.0.0.0",
    val isLocal : Boolean = false,
) {
    var diskSize: Long = 0
    var diskFree: Long = 0
    var lastReport: ZonedDateTime = TimeZero
    var actimetreList = mutableMapOf<Int, Actimetre>()

    fun toCentral(): ActiserverShort {
        lastReport = now()
        return ActiserverShort().init(this)
    }

    fun df(size:Long, free:Long) {
        diskSize = size
        diskFree = free
    }

    fun updateActimetre(actimId: Int, mac: String, boardType: String, version: String, bootTime: ZonedDateTime, sensorBits: UByte): Actimetre {
        synchronized(this) {
            var a = actimetreList[actimId]
            if (a == null) {
                a = Actimetre(actimId, serverId = serverId)
                actimetreList[actimId] = a
            }
            a.setInfo(mac, boardType, version, bootTime = bootTime, sensorBits = sensorBits)
            return a
        }
    }

    fun removeActim(actimId: Int) {
        synchronized(this) {
            actimetreList.remove(actimId)
        }
    }

    fun serverName(): String {
        return "Actis%03d".format(serverId)
    }
}
