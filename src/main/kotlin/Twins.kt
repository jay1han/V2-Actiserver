
import kotlinx.serialization.Required
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.IOException
import java.net.SocketException
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousCloseException
import java.nio.channels.ByteChannel
import java.nio.channels.ClosedChannelException
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.io.path.Path
import kotlin.io.path.fileSize
import kotlin.io.path.forEachDirectoryEntry

class Record(buffer: ByteArray, val sensorId: String, bootEpoch: Long, msgBootEpoch: Long, msgMillis: Int) {
    private val diffMillis = buffer[0].toUByte().toInt() * 256 + buffer[1].toUByte().toInt()
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

    private fun makeInt(msb: Byte, lsb: Byte) : Int {
        var integer = msb.toUByte().toInt() * 256 + lsb.toUByte().toInt()
        if (integer >= 32768) integer -= 65536
        return integer
    }
    private fun makeAccelStr(buffer: ByteArray): String{
        val rawX = makeInt(buffer[0], buffer[1])
        val rawY = makeInt(buffer[2], buffer[3])
        val rawZ = makeInt(buffer[4], buffer[5])
        return arrayOf(
            rawX / 8192.0f,
            rawY / 8192.0f,
            rawZ / 8192.0f
        ).joinToString(separator = " ") { "%+7.4f".format(it) }
    }

    private fun makeGyroStr(buffer: ByteArray): String {
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

    private fun findDataFile(atDateTime: ZonedDateTime) {
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
        } else {
            fileName = lastRepoFile
            fileDate = lastRepoDate
            fileSize = lastRepoSize
            fileHandle = BufferedWriter(FileWriter(lastRepoFile.fullName(), true), 16384)
            mqttLog("Continue data file $lastRepoFile")
        }
    }

    private fun newDataFile(atDateTime: ZonedDateTime) {
        fileName = sensorName() + "_" + atDateTime.actiFormat() + ".txt"
        fileDate = atDateTime
        fileSize = 0
        fileHandle = BufferedWriter(FileWriter(fileName.fullName(), false), 16384)
        mqttLog("Start data file $fileName")
    }

    fun writeData(record: Record): Int {
        if (!this::fileHandle.isInitialized) findDataFile(record.dateTime)
        else if (fileSize > MAX_REPO_SIZE ||
            Duration.between(fileDate, record.dateTime) > MAX_REPO_TIME
        ) {
            fileHandle.close()
            newDataFile(record.dateTime)
        }
        fileHandle.append(record.textStr + "\n")
        fileSize += record.textStr.length + 1
        return record.textStr.length + 1
    }

    fun flushIfOpen() {
        if (this::fileHandle.isInitialized) {
            try {
                fileHandle.flush()
            } catch(e: IOException) {}
        }
    }

    fun closeIfOpen() {
        if (this::fileHandle.isInitialized) {
            try {
                fileHandle.close()
            } catch (e: IOException) {}
        }
    }
}

val Frequencies = listOf(50, 100, 1, 200, 30, 10)

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
        repoSize = a.repoSize
        return this
    }
}

class Actimetre(
    val actimId   : Int = 9999,
    var mac       : String = "............",
    var boardType : String = "???",
    var version   : String = "000",
    val serverId  : Int = 0,
) {
    var isDead = false
    var bootTime = TimeZero
    var lastSeen = TimeZero
    var lastReport = TimeZero
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
    var repoSize: Long = 0

    private fun toCentral(): ActimetreShort {
        return ActimetreShort().init(this)
    }

    fun run(channel: ByteChannel) {
        this.channel = channel
        while (true) {
            val sensorBuffer = ByteBuffer.allocate(msgLength)
            var inputLen: Int
            try {
                inputLen = this.channel.read(sensorBuffer)
                while (inputLen < msgLength) {
                    inputLen += this.channel.read(sensorBuffer)
                }
            } catch (e: AsynchronousCloseException) {
                printLog("${actimName()} Asynchronous Close")
                return
            } catch (e: ClosedChannelException) {
                printLog("${actimName()} Closed Channel")
                return
            } catch (e: SocketException) {
                printLog("${actimName()} Socket")
                return
            }

            val sensor = sensorBuffer.array()
            val msgBootEpoch = sensor.getInt3At(0)
            val msgMillis = (sensor[3].toUByte().toInt() and 0x03) * 256 +
                    sensor[4].toUByte().toInt()
            val msgDateTime = ZonedDateTime.ofInstant(
                Instant.ofEpochSecond(msgBootEpoch + bootEpoch, msgMillis * 1_000_000L),
                ZoneId.of("Z"))
            if (lastMessage == TimeZero) {
                totalPoints = 1
                missingPoints = 0

                Path(REPO_ROOT).forEachDirectoryEntry("${actimName()}*") {
                    repoSize += it.fileSize()
                }
            } else {
                val cycles = Duration.between(lastMessage, msgDateTime)
                    .dividedBy(Duration.ofNanos(cycleNanoseconds))
                    .toInt()
                if (cycles > 1) {
                    missingPoints += cycles - 1
                    printLog("${actimName()} missed cycle $missingPoints / $totalPoints = ${missingPoints / totalPoints}")
                }
                totalPoints += cycles
                rating = missingPoints.toDouble() / totalPoints.toDouble()
            }
            lastMessage = msgDateTime.minusNanos(cycleNanoseconds / 10L)

            val msgOOBD = sensor[3].toUByte().toInt() shr 2
            val msgFrequency = msgOOBD and 0x07
            frequency = Frequencies[msgFrequency]
            cycleNanoseconds = 1_000_000_000L / frequency

            var index = 5
            while (index < msgLength) {
                val record = Record(sensor.sliceArray(index until (index + DATA_LENGTH)),
                    sensorOrder[index / DATA_LENGTH], bootEpoch, msgBootEpoch, msgMillis)
                if (!sensorList.containsKey(record.sensorId))
                    sensorList[record.sensorId] = SensorInfo(actimId, record.sensorId)
                repoSize += sensorList[record.sensorId]!!.writeData(record)
                index += DATA_LENGTH
            }
            lastSeen = now()
        }
    }

    fun dies() {
        if (isDead) return
        isDead = true
        val reqString = CENTRAL_BIN + "action=actimetre-off" +
                "&serverId=${serverId}&actimId=${actimId}"
        sendHttpRequest(reqString)
        mqttLog("${actimName()} dies")
        for (sensorInfo in sensorList.values) {
            sensorInfo.closeIfOpen()
        }
        Self.removeActim(actimId)
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
        if (lastSeen == TimeZero) return
        if (Duration.between(lastSeen, now) > ACTIM_DEAD_TIME) {
            if (isDead) return
            mqttLog("Killing ${actimName()}")
            if (this::channel.isInitialized) channel.close()
            else dies()
        } else if (Duration.between(lastReport, now) > ACTIM_REPORT_TIME) {
            val reqString = CENTRAL_BIN + "action=actimetre" +
                    "&serverId=${serverId}&actimId=${actimId}"
            sendHttpRequest(reqString, Json.encodeToString(toCentral()))
            lastReport = now
            sensorList.values.map {it.flushIfOpen()}
        }
    }

    fun setInfo(mac: String, boardType: String, version: String, bootTime: ZonedDateTime, sensorBits: Byte) {
        this.mac = mac
        this.boardType = boardType
        this.version = version
        this.bootTime = bootTime
        lastSeen = TimeZero
        lastReport = TimeZero
        totalPoints = 0
        missingPoints = 0
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

@Serializable
class ActiserverShort(
    @Required var serverId: Int = 0,
    @Required var machine : String = "Unknown",
    @Required var version : String = "000",
    @Required var channel : Int = 0,
    @Required var ip: String = "0.0.0.0",
    @Serializable(with = DateTimeAsString::class)
    @Required var lastReport : ZonedDateTime = TimeZero,
    @Required var actimetreList : Set<Int> = setOf(),
) {
    fun init(s: Actiserver) : ActiserverShort {
        serverId = s.serverId
        machine = s.machine
        version = s.version
        channel = s.channel
        ip = s.ip
        lastReport = s.lastReport
        actimetreList = s.actimetreList.keys.toSet()
        return this
    }
}

class Actiserver(
    val serverId: Int = 0,
    val machine : String = "Unknown",
    val version : String = "000",
    val channel : Int = 0,
    val ip      : String = "0.0.0.0",
) {
    var lastReport = TimeZero
    var actimetreList = mutableMapOf<Int, Actimetre>()

    fun toCentral(): ActiserverShort {
        return ActiserverShort().init(this)
    }

    fun updateActimetre(actimId: Int, mac: String, boardType: String, version: String, bootTime: ZonedDateTime, sensorBits: Byte): Actimetre {
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
}
