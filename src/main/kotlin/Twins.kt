

import kotlinx.serialization.Required
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import mqtt.packets.Qos
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.Writer
import java.net.SocketException
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousCloseException
import java.nio.channels.ByteChannel
import java.nio.channels.ClosedChannelException
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.concurrent.thread
import kotlin.io.path.Path

class Record(buffer: ByteArray, val sensorId: String, bootEpoch: Int, msgBootEpoch: Int, msgMillis: Int) {
    private val diffMillis = buffer[0] * 256 + buffer[1]
    private val adjEpoch = if (msgMillis + diffMillis > 1000) 1 else 0
    val dateTime = ZonedDateTime.ofInstant(
        Instant.ofEpochSecond((msgBootEpoch + bootEpoch + adjEpoch).toLong(),
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

    private fun actimNum(): String {return "%04d".format(actimId)}

    private fun newDataFile(atDateTime: ZonedDateTime) {
        fileName = sensorName() + "_" + atDateTime.actiFormat() + ".txt"
        fileDate = atDateTime
        fileSize = 0
        fileHandle = Files.newBufferedWriter(Path(fileName.DATAname()),
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
        printLog("Start Data file $fileName")
    }

    fun writeData(record: Record) {
        if (fileName == "") newDataFile(record.dateTime)
        if (fileSize > UPLOAD_SIZE ||
            Duration.between(fileDate, record.dateTime) > UPLOAD_TIME
        ) {
            uploadFile(fileHandle as Writer, fileName)
            newDataFile(record.dateTime)
        }
        if (options.fullText) {
            mqttClient.publish(false, Qos.AT_MOST_ONCE, "$MQTT_TEXT/${actimNum()}/$sensorId",
                record.textStr.toByteArray().toUByteArray())
        }
        fileHandle.append(record.textStr + "\n")
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

class Actimetre(
    val actimId   : Int = 9999,
    var mac       : String = "............",
    var boardType : String = "???",
    val serverId  : Int = 0,
) {
    var isDead = false
    var bootTime = TimeZero
    var lastSeen = TimeZero
    var lastReport = TimeZero
    private var sensorList = mutableMapOf<String, SensorInfo>()
    private var nSensors = 0
    private var channel: ByteChannel? = null
    private var errors: Int = 0
    private var msgLength = 0
    private var sensorOrder = mutableListOf<String>()
    private var bootEpoch = 0

    private fun toCentral(): ActimetreShort {
        return ActimetreShort().init(this)
    }

    fun run(channel: ByteChannel) {
        this.channel = channel
        while (true) {
            val sensorBuffer = ByteBuffer.allocate(msgLength)
            var inputLen = 0
            try {
                while (inputLen < msgLength) {
                    inputLen += this.channel!!.read(sensorBuffer)
//                    Thread.yield()
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
            val msgMillis = sensor[3] * 256 + sensor[4]
            var index = 5
            while (index < msgLength) {
                val record = Record(sensor.sliceArray(index until (index + DATA_LENGTH)),
                    sensorOrder[index / DATA_LENGTH], bootEpoch, msgBootEpoch, msgMillis)
                if (!sensorList.containsKey(record.sensorId))
                    sensorList[record.sensorId] = SensorInfo(actimId, record.sensorId)
                sensorList[record.sensorId]!!.writeData(record)
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
            uploadFile(sensorInfo.fileHandle, sensorInfo.fileName)
        }
        Self.removeActim(actimId)
        channel?.close()
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
        if (Duration.between(lastSeen, now) > ACTIM_DEAD_TIME) {
            if (isDead) return
            mqttLog("Killing ${actimName()}")
            channel?.close()
//            dies()
        } else if (Duration.between(lastReport, now) > ACTIM_REPORT_TIME) {
            val reqString = CENTRAL_BIN + "action=actimetre" +
                    "&serverId=${serverId}&actimId=${actimId}"
            sendHttpRequest(reqString, Json.encodeToString(toCentral()))
//            mqttLog("${actimName()} is alive")
            lastReport = now
            errors = 0
        }
    }

    fun setInfo(mac: String, boardType: String, bootTime: ZonedDateTime, lastSeen: ZonedDateTime, sensorBits: Byte) {
        this.mac = mac
        this.boardType = boardType
        this.bootTime = bootTime
        bootEpoch = bootTime.toEpochSecond().toInt()
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
    @Required var channel : Int = 999,
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
        channel = s.channel
        started = s.started
        lastReport = s.lastReport
        actimetreList = s.actimetreList.keys.toSet()
        return this
    }
}

class Actiserver(
    val serverId: Int = 0,
    val mac     : String = "............",
    val ip      : String = "0.0.0.0",
    val channel : Int = 999,
    val started : ZonedDateTime = TimeZero
) {
    var lastReport = TimeZero
    var actimetreList = mutableMapOf<Int, Actimetre>()

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
