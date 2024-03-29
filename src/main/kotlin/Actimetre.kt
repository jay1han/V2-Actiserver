@file:OptIn(ExperimentalUnsignedTypes::class)

import kotlinx.serialization.Required
import kotlinx.serialization.Serializable
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

val Frequencies = listOf(50, 100, 1, 200, 30, 10)
val FrequenciesV3 = listOf(100, 500, 1000, 2000, 4000, 8000)

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
    private var v3 = false
    var isDead = false
    var bootTime: ZonedDateTime = TimeZero
    var lastSeen: ZonedDateTime = TimeZero
    var lastReport: ZonedDateTime = TimeZero
    private var sensorList = mutableMapOf<String, SensorInfo>()
    private var nSensors = 0
    lateinit var channel: ByteChannel
    private var msgLength = 0
    private var sensorOrder = mutableListOf<String>()
    private var bootEpoch = 0L
    var frequency = 100
    private var cycleNanoseconds = 1_000_000_000L / frequency
    private var lastMessage = TimeZero
    private var missingPoints  = 0
    private var totalPoints = 0
    var rating = 0.0
    var rssi: Int = 0
    var samplingMode: Int = 0
    var repoNums: Int = 0
    var repoSize: Long = 0

    private fun readInto(buffer: ByteBuffer): Int {
        var inputLen = 0
        val timeout = now().plusSeconds(1)
        try {
            while (inputLen < buffer.capacity() && now().isBefore(timeout)) {
                inputLen += this.channel.read(buffer)
            }
        } catch (e: Throwable) {
            printLog("${actimName()}:$e")
            return 0
        }
        if (inputLen != buffer.capacity()) {
            printLog("${actimName()} sent $inputLen bytes < ${buffer.capacity()}")
        }
        return inputLen
    }

    fun run(channel: ByteChannel) {
        this.channel = channel
        if (v3) {
            while (true) {
                val headerBuffer = ByteBuffer.allocate(HEADERV3_LENGTH)
                val inputLen = readInto(headerBuffer)
                if (inputLen != HEADERV3_LENGTH) {
                    printLog("${actimName()} received header $inputLen bytes != $HEADERV3_LENGTH")
                    break
                }

                lastSeen = now()
                val sensorInfo = headerBuffer.array().toUByteArray()
                if (sensorInfo[0].toInt() == 0xFF) {
                    val messageLen = sensorInfo[3].toInt()
                    val messageBuffer = ByteBuffer.allocate(messageLen)
                    readInto(messageBuffer)
                    val messageText = messageBuffer.array().decodeToString()
                    printLog("${actimName()} FATAL:$messageText")
                    break
                }
                val msgBootEpoch = sensorInfo.getInt3At(0).toLong()
                val count = sensorInfo[3].toInt() and 0x3F
                val sensorName = "${'1' + (sensorInfo[3].toInt() shr 7)}${'A' + ((sensorInfo[3].toInt() and 0x40) shr 6)}"
                val msgMicros = sensorInfo.getInt3At(5).toLong()
                val msgDateTime = ZonedDateTime.ofInstant(
                    Instant.ofEpochSecond(bootEpoch + msgBootEpoch, msgMicros * 1000L),
                    ZoneId.of("Z")
                )

                val msgFrequency = sensorInfo[4].toInt() and 0x07
                if (msgFrequency >= FrequenciesV3.size) {
                    printLog("${actimName()} Frequency code $msgFrequency out of bounds")
                    break
                }
                frequency = FrequenciesV3[msgFrequency]
                cycleNanoseconds = 1_000_000_000L / frequency

                rssi = (sensorInfo[4].toInt() shr 5) and 0x07
                samplingMode = (sensorInfo[4].toInt() shr 3) and 0x03
                val dataLength = when (samplingMode) {
                    1 -> 6
                    2 -> 4
                    else -> 10
                }

                if (lastMessage == TimeZero) {
                    totalPoints = 1
                    missingPoints = 0

                    Path(REPO_ROOT).forEachDirectoryEntry("${actimName()}*") {
                        repoSize += it.fileSize()
                        repoNums++
                    }
                } else {
                    val cycles = Duration.between(lastMessage, msgDateTime)
                        .dividedBy(Duration.ofNanos(cycleNanoseconds))
                        .toInt()
                    totalPoints += cycles
                    if (cycles > count) {
                        missingPoints += cycles - count
//                        printLog("${actimName()} missed ${cycles - count} cycles $missingPoints / $totalPoints = ${missingPoints.toDouble() / totalPoints}")
                    }
                    rating = missingPoints.toDouble() / totalPoints.toDouble()
                }
                lastMessage = msgDateTime.minusNanos(cycleNanoseconds / 10L)

                val sensorBuffer = ByteBuffer.allocate(dataLength * count)
                val dataLen = readInto(sensorBuffer)
                if (dataLen != dataLength * count) {
                    printLog("${actimName()} Data length $dataLen != ${dataLength * count}")
                    break
                }

                val sensorData = sensorBuffer.array().toUByteArray()
                for (index in 0 until count) {
                    val record = RecordV3(samplingMode,
                        sensorData.sliceArray(index * dataLength until (index + 1) * dataLength),
                        bootEpoch, msgBootEpoch,
                        msgMicros - ((count - index - 1) * cycleNanoseconds / 1000).toInt()
                    )
                    if (!sensorList.containsKey(sensorName))
                        sensorList[sensorName] = SensorInfo(actimId, sensorName)
                    val (newFile, sizeWritten) = sensorList[sensorName]!!.writeData(record)
                    repoSize += sizeWritten
                    if (newFile) repoNums++
                    if (newFile or (repoSize % 100_000 < 64)) {
                        htmlData()
                    }
                }
            }
        } else {
            while (true) {
                val sensorBuffer = ByteBuffer.allocate(msgLength)
                val dataLen = readInto(sensorBuffer)
                if (dataLen != msgLength) {
                    printLog("Data length $dataLen != $msgLength")
                    break
                }

                lastSeen = now()
                val sensorData = sensorBuffer.array().toUByteArray()
                val msgBootEpoch = sensorData.getInt3At(0).toLong()
                val msgMillis = (sensorData[3].toLong() and 0x03) * 256 +
                        sensorData[4].toLong()
                val msgDateTime = ZonedDateTime.ofInstant(
                    Instant.ofEpochSecond(bootEpoch + msgBootEpoch, msgMillis * 1_000_000L),
                    ZoneId.of("Z")
                )

                val msgFrequency = (sensorData[3].toInt() shr 2) and 0x07
                frequency = Frequencies[msgFrequency]
                cycleNanoseconds = 1_000_000_000L / frequency

                rssi = (sensorData[3].toInt() shr 5) and 0x07

                if (lastMessage == TimeZero) {
                    totalPoints = 1
                    missingPoints = 0

                    Path(REPO_ROOT).forEachDirectoryEntry("${actimName()}*") {
                        repoSize += it.fileSize()
                        repoNums++
                    }
                } else {
                    val cycles = Duration.between(lastMessage, msgDateTime)
                        .dividedBy(Duration.ofNanos(cycleNanoseconds))
                        .toInt()
                    if (cycles > frequency) {
//                        printLog("${actimName()} jumped over ${cycles - 1} cycles")
                    }
                    totalPoints += cycles
                    if (cycles > 1) {
                        missingPoints += cycles - 1
//                        printLog("${actimName()} missed $cycles cycles $missingPoints / $totalPoints = ${missingPoints.toDouble() / totalPoints}")
                    }
                    rating = missingPoints.toDouble() / totalPoints.toDouble()
                }
                lastMessage = msgDateTime.minusNanos(cycleNanoseconds / 10L)

                var index = HEADER_LENGTH
                while (index < msgLength) {
                    val record = Record(
                        sensorData.sliceArray(index until (index + DATA_LENGTH)),
                        sensorOrder[(index - HEADER_LENGTH) / DATA_LENGTH], bootEpoch, msgBootEpoch, msgMillis
                    )
                    if (!sensorList.containsKey(record.sensorId))
                        sensorList[record.sensorId] = SensorInfo(actimId, record.sensorId)
                    val (newFile, sizeWritten) = sensorList[record.sensorId]!!.writeData(record)
                    repoSize += sizeWritten
                    if (newFile) repoNums++
                    if (newFile or (repoSize % 100_000 < 64)) {
                        htmlData()
                    }
                    index += DATA_LENGTH
                }
            }
        }
    }

    fun htmlData() {
        val repoList: MutableMap<String, MutableList<String>> = mutableMapOf()
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
            for (line in lines.slice(1 until lines.size)) {
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
        v3 = version >= "300"
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
                val sensorName = "%d%c".format(port + 1, 'A' + address)
                if ((sensorBits.toInt() and bitMask) != 0) {
                    sensorList[sensorName] = SensorInfo(actimId, sensorName)
                    nSensors += 1
                    sensorOrder.add(sensorName)
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

