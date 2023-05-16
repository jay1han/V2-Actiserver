

import kotlinx.serialization.Required
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import mqtt.packets.Qos
import java.io.BufferedWriter
import java.net.SocketException
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousCloseException
import java.nio.channels.ByteChannel
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.io.path.Path

class Record(buffer: ByteArray, val sensorId: String, bootTime: ZonedDateTime, thisEpoch: Long, msgMillis: Long) {
    private val bootEpoch = bootTime.toEpochSecond()
    private val diffMillis = buffer[0] * 256 + buffer[1]
    private val adjEpoch = if (msgMillis + diffMillis > 1000) 1 else 0
    val dateTime = ZonedDateTime.ofInstant(
        Instant.ofEpochSecond(thisEpoch + bootEpoch + adjEpoch,
            (((msgMillis + diffMillis) % 1000) * 1000000)),
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
    @Transient private lateinit var fileHandle: BufferedWriter

    private fun sensorName(): String {return "Actim%04d-%s".format(actimId, sensorId)}

    private fun actimNum(): String {return "%04d".format(actimId)}

    private fun newDataFile(atDateTime: ZonedDateTime) {
        fileName = sensorName() + "_" + atDateTime.actiFormat() + ".txt"
        fileDate = atDateTime
        fileSize = 0
        fileHandle = Files.newBufferedWriter(Path(fileName.DATAname()),
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.DSYNC)
    }

    fun writeData(record: Record) {
        if (fileName == "") newDataFile(record.dateTime)
        if (fileSize > UPLOAD_SIZE ||
            Duration.between(fileDate, record.dateTime) > UPLOAD_TIME) {
            fileHandle.close()
            uploadFile(fileName)
            newDataFile(record.dateTime)
        }
        if (options.fullText) {
            mqttClient.publish(false, Qos.AT_MOST_ONCE, "$MQTT_TEXT/${actimNum()}/$sensorId",
                record.textStr.toByteArray().toUByteArray())
        }
        fileHandle.append(record.textStr + "\n")
        fileHandle.flush()
        fileSize += record.textStr.length + 1
    }
}

@Serializable
class ActimetreShort(
    @Required var actimId           : Int = 9999,
    @Required private var mac       : String = "............",
    @Required private var boardType : String = "???",
    @Required private var serverId  : Int = 0,
    @Required private var isDead    : Boolean = false,
    @Serializable(with = DateTimeAsString::class)
    @Required var bootTime          : ZonedDateTime = TimeZero,
    @Serializable(with = DateTimeAsString::class)
    @Required var lastSeen          : ZonedDateTime = TimeZero,
    @Serializable(with = DateTimeAsString::class)
    @Required var lastReport        : ZonedDateTime = TimeZero,
    @Required var sensorStr         : String = ""
) {
    fun init(a: Actimetre): ActimetreShort {
        actimId = a.actimId
        mac = a.mac
        boardType = a.boardType
        serverId = a.serverId
        isDead = a.isDead
        bootTime = a.bootTime
        lastSeen = a.lastSeen
        lastReport = a.lastReport
        sensorStr = a.sensorStr()
        return this
    }
}

@Serializable
class Actimetre(
    @Required val actimId   : Int = 9999,
    @Required var mac       : String = "............",
    @Required var boardType : String = "???",
    @Required val serverId  : Int = 0,
) {
    @Required var isDead = false
    @Serializable(with = DateTimeAsString::class)
    @Required var bootTime = TimeZero
    @Serializable(with = DateTimeAsString::class)
    @Required var lastSeen = TimeZero
    @Serializable(with = DateTimeAsString::class)
    @Required var lastReport = TimeZero
    @Required var sensorList = mutableMapOf<String, SensorInfo>()
    @Transient var nSensors = 0
    @Transient var channel: ByteChannel? = null
    @Transient var errors: Int = 0
    @Transient var msgLength = 0
    @Transient var sensorOrder = mutableListOf<String>()

    private fun toCentral(): ActimetreShort {
        return ActimetreShort().init(this)
    }

    fun run(channel: ByteChannel) {
        this.channel = channel
        val sensorBuffer = ByteBuffer.allocate(msgLength)
        var inputLen = 0
        while (true) {
            try {
                while (inputLen < msgLength)
                    inputLen += this.channel!!.read(sensorBuffer)
            } catch (e: AsynchronousCloseException) {
                printLog("${actimName()} AsynchronousCloseException")
                return
            } catch (e: SocketException) {
                printLog("${actimName()} SocketException")
                return
            }

            val sensor = sensorBuffer.array()
            val thisEpoch = sensor.getInt3At(0)
            val thisMillis = sensor[3] * 256 + sensor[4]
            var index = 5
            while (index < msgLength) {
                val record = Record(sensor.sliceArray(index until (index + DATA_LENGTH)),
                    sensorOrder[index / DATA_LENGTH], bootTime, thisEpoch.toLong(), thisMillis.toLong())
                if (!sensorList.containsKey(record.sensorId))
                    sensorList[record.sensorId] = SensorInfo(actimId, record.sensorId)
                sensorList[record.sensorId]!!.writeData(record)
                index += DATA_LENGTH
            }
            lastSeen = now()
        }
    }

    fun dies() {
        synchronized(this) {
            if (isDead) return
            isDead = true
            for (sensorInfo in sensorList.values) {
                uploadFile(sensorInfo.fileName)
            }
            mqttLog("${actimName()} dies")
            val reqString = CENTRAL_BIN + "action=actimetre-off" +
                    "&serverId=${serverId}&actimId=${actimId}"
            sendHttpRequest(reqString)
            Self.removeActim(actimId)
        }
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
        synchronized(this) {
            if (Duration.between(lastSeen, now) > ACTIM_DEAD_TIME) {
                dies()
            } else if (Duration.between(lastReport, now) > ACTIM_REPORT_TIME) {
                val reqString = CENTRAL_BIN + "action=actimetre" +
                        "&serverId=${serverId}&actimId=${actimId}"
                sendHttpRequest(reqString, Json.encodeToString(toCentral()))
                mqttLog("${actimName()} reported")
                lastReport = now
                errors = 0
            }
        }
    }

    fun setInfo(mac: String, boardType: String, bootTime: ZonedDateTime, lastSeen: ZonedDateTime, sensorBits: Byte) {
        this.mac = mac
        this.boardType = boardType
        this.bootTime = bootTime
        this.lastSeen = lastSeen
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
    @Required var mac     : String = "............",
    @Required var ip      : String = "0.0.0.0",
    @Serializable(with = DateTimeAsString::class)
    @Required var started : ZonedDateTime = TimeZero,
    @Serializable(with = DateTimeAsString::class)
    @Required var lastReport : ZonedDateTime = TimeZero,
    @Required var actimetreList : Set<Int> = setOf(),
) {
    fun init(s: Actiserver) : ActiserverShort {
        serverId = s.serverId
        mac = s.mac
        ip = s.ip
        started = s.started
        lastReport = s.lastReport
        actimetreList = s.actimetreList.keys.toSet()
        return this
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

    fun toCentral(): ActiserverShort {
        return ActiserverShort().init(this)
    }

    fun updateActimetre(actimId: Int, mac: String, boardType: String, bootTime: ZonedDateTime, sensorBits: Byte): Actimetre {
         synchronized(this) {
            var a = actimetreList[actimId]
            if (a == null) {
                a = Actimetre(actimId, serverId = serverId)
                actimetreList[actimId] = a
            }
            a.setInfo(mac, boardType, bootTime = bootTime, lastSeen = bootTime, sensorBits = sensorBits)
            return a
        }
    }

    fun removeActim(actimId: Int) {
        synchronized(this) {
            actimetreList.remove(actimId)
        }
    }
}
