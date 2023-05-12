@file:OptIn(ExperimentalUnsignedTypes::class)

import kotlinx.coroutines.sync.Mutex
import kotlinx.serialization.Required
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import java.nio.channels.SocketChannel
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import kotlin.io.path.Path

class Record(buffer: ByteArray) {
    private val port: Int = (buffer[0].toInt() shr 4) and 0x03
    private val address: Int = (buffer[0].toInt() and 0x0F)
    val sensorId = "${port + 1}${'A' + address}"
    private val epochSeconds = buffer.getIntAt(1).toLong()
    private val microSeconds = buffer[5].toLong() * 65536 + buffer[6].toLong() * 256 + buffer[7].toLong()
    val dateTime: ZonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(
        epochSeconds, microSeconds * 1000), ZoneOffset.UTC)
    private val accelStr = makeAccelStr(buffer.sliceArray(8..13))
    private val gyroStr = makeGyroStr(buffer.sliceArray(14..17))
    val textStr: String = dateTime.prettyFormat() +
            ".%03d,%03d ".format(dateTime.nano / 1000000L, (dateTime.nano / 1000) % 1000) +
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
){
    private fun sensorName(): String {return "Actim%04d-%s".format(actimId, sensorId)}

    private fun newDataFile(atDateTime: ZonedDateTime) {
        fileName = sensorName() + "_" + atDateTime.actiFormat() + ".txt"
        fileSize = 0
    }

    fun writeData(record: Record) {
        if (fileName == "") newDataFile(record.dateTime)
        if (fileSize > UPLOAD_SIZE ||
            Duration.between(this.getFileDate(), record.dateTime) > UPLOAD_TIME) {
            uploadFile(fileName)
            newDataFile(record.dateTime)
        }
        printLog("Writing data to ${fileName.DATAname()}")
        val outfile = Files.newBufferedWriter(Path(fileName.DATAname()),
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.DSYNC)
        outfile.append(record.textStr + "\n")
        outfile.flush()
        outfile.close()
        fileSize += record.textStr.length + 1
    }

    private fun getFileDate(): ZonedDateTime {
        return fileName.parseFileDate()
    }
}

enum class ActimState {NULL, Running, MustReport, Dead}

@Serializable
class Actimetre(
    @Required val actimId   : Int = 9999,
    @Required private var mac       : String = "............",
    @Required private var boardType : String = "???",
    @Required private val serverId  : Int = 0,
) {
    @Required private var isDead   = false
    @Serializable(with = DateTimeAsString::class)
    @Required var bootTime = TimeZero
    @Serializable(with = DateTimeAsString::class)
    @Required var lastSeen = TimeZero
    @Serializable(with = DateTimeAsString::class)
    @Required var lastReport = TimeZero
    @Required var sensorList = mutableMapOf<String, SensorInfo>()
    @Transient var channel: SocketChannel? = null
    // We're going to manage the channel from within the Actimetre class


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

    fun addRecord(record: Record) {
        printLog("Add to Actim${actimId}: " + record.textStr)
        if (!sensorList.containsKey(record.sensorId))
            sensorList[record.sensorId] = SensorInfo(actimId, record.sensorId)
        sensorList[record.sensorId]!!.writeData(record)
        seen()
    }

    fun loopOk(now: ZonedDateTime): ActimState {
        if (Duration.between(lastSeen, now) > ACTIM_DEAD_TIME) {
            printLog("${actimName()} detected dead")
            isDead = true
            for (sensorInfo in sensorList.values) {
                uploadFile(sensorInfo.fileName)
            }
            return ActimState.Dead
        } else {
            if (Duration.between(lastReport, now) > ACTIM_REPORT_TIME) {
                lastReport = now
                return ActimState.MustReport
            }
            return ActimState.Running
        }
    }

    fun setInfo(mac: String, boardType: String, bootTime: ZonedDateTime) {
        this.mac = mac
        this.boardType = boardType
        this.bootTime = bootTime
    }

    fun seen(now: ZonedDateTime = now()) {
        lastSeen = now
    }

    fun actimName(): String {
        return "Actim%04d".format(actimId)
    }
}

@Serializable
class Actiserver(
    @Required val serverId: Int = 0,
    @Required val mac     : String = "............",
    @Required val ip      : String = "0.0.0.0",
    @Serializable(with = DateTimeAsString::class)
    @Required val started : ZonedDateTime = TimeZero
) {
    @Serializable(with = DateTimeAsString::class)
    @Required var lastReport = TimeZero
    @Required var actimetreList = mutableMapOf<Int, Actimetre>()
    @Transient var mutex: Mutex = Mutex()

    fun updateActimetre(actimId: Int, mac: String, boardType: String, bootTime: ZonedDateTime) {
        var a = actimetreList[actimId]
        if (a == null) {
            a = Actimetre(actimId, serverId = serverId)
            actimetreList[actimId] = a
        }
        a.setInfo(mac, boardType, bootTime)
        a.seen()
    }
}
