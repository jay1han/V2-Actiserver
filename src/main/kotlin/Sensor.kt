@file:OptIn(ExperimentalUnsignedTypes::class)

import kotlinx.serialization.Required
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.io.path.Path
import kotlin.io.path.fileSize
import kotlin.io.path.forEachDirectoryEntry
import kotlin.math.pow
import kotlin.math.sqrt


private fun makeInt(msb: UByte, lsb: UByte) : Int {
    var integer = msb.toInt() * 256 + lsb.toInt()
    if (integer >= 32768) integer -= 65536
    return integer
}

class AccelData {
    private var rawX: Float = 0.0f
    private var rawY: Float = 0.0f
    private var rawZ: Float = 0.0f
    var rawStr = ""
    private var vec: Float = 0.0f
    var vecStr = ""

    fun read(buffer: UByteArray): AccelData {
        rawX = makeInt(buffer[0], buffer[1]) / 8192.0f
        rawY = makeInt(buffer[2], buffer[3]) / 8192.0f
        rawZ = makeInt(buffer[4], buffer[5]) / 8192.0f
        if (OUTPUT_RAW) {
            rawStr = "," + arrayOf(rawX, rawY, rawZ).joinToString(separator = ",") { "%+.4f".format(it) }
        }
        if (OUTPUT_VECTORS) {
            vec = sqrt(rawX.pow(2) + rawY.pow(2) + rawZ.pow(2))
            vecStr = ",%.5f".format(vec)
        }
        return this
    }
}

class GyroData {
    private var rawX = 0.0f
    private var rawY = 0.0f
    private var rawZ = 0.0f
    var rawStr = ""
    private var vec = 0.0f
    var vecStr = ""

    fun read(buffer: UByteArray): GyroData {
        rawX = makeInt(buffer[0], buffer[1]) / 131.0f
        rawY = makeInt(buffer[2], buffer[3]) / 131.0f
        rawZ = if (buffer.size > 4) makeInt(buffer[4], buffer[5]) / 131.0f else 0.0f
        if (OUTPUT_RAW) {
            rawStr = "," +
                    if (INCLUDE_GZ) {
                        arrayOf(rawX, rawY, rawZ).joinToString(separator = ",") { "%+.3f".format(it) }
                    } else {
                        arrayOf(rawX, rawY).joinToString(separator = ",") { "%+.3f".format(it) }
                    }
        }
        if (OUTPUT_VECTORS) {
            vec = sqrt(rawX.pow(2) + rawY.pow(2) + rawZ.pow(2))
            vecStr = ",%.4f".format(vec)
        }
        return this
    }
}

class Record(buffer: UByteArray, val sensorId: String, bootEpoch: Long, msgBootEpoch: Long, msgMillis: Long) {
    private val diffMillis = buffer[0].toLong() * 256 + buffer[1].toLong()
    val dateTime: ZonedDateTime = ZonedDateTime.ofInstant(
        Instant.ofEpochSecond(bootEpoch + msgBootEpoch,
            (msgMillis + diffMillis) * 1_000_000L),
        ZoneId.of("Z"))
    private val accelStr = AccelData().read(buffer.sliceArray(2..7)).rawStr
    private val gyroStr = GyroData().read(buffer.sliceArray(8..11)).rawStr
    val textStr: String = dateTime.csvFormat() +
            ".%03d".format(dateTime.nano / 1000000L) +
            accelStr + gyroStr
}

class RecordV3(
    v34: Boolean,
    samplingMode: Int,
    buffer: UByteArray,
    bootEpoch: Long,
    msgBootEpoch: Long,
    msgMicros: Long
) {
    val dateTime: ZonedDateTime = ZonedDateTime.ofInstant(
        Instant.ofEpochSecond(bootEpoch + msgBootEpoch,
            msgMicros * 1_000L),
        ZoneId.of("Z"))

    var textStr: String

    init {
        val accel = AccelData()
        val gyro = GyroData()

        when (samplingMode) {
            1 -> accel.read(buffer.sliceArray(0..5))
            2 -> {
                if (v34) gyro.read(buffer.sliceArray(0..5))
                else gyro.read(buffer.sliceArray(0..3))
            }
            else -> {
                accel.read(buffer.sliceArray(0..5))
                if (v34) gyro.read(buffer.sliceArray(6..11))
                else gyro.read(buffer.sliceArray(6..9))
            }
        }
        textStr = dateTime.csvFormat() +
                ".%06d".format(dateTime.nano / 1_000L)
        if (OUTPUT_RAW) textStr += accel.rawStr + gyro.rawStr
        if (OUTPUT_VECTORS) textStr += accel.vecStr + gyro.vecStr
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
    @Transient private var lastDateTime: ZonedDateTime = TimeZero

    private fun sensorName(): String {return "Actim%04d-%s".format(actimId, sensorId.uppercase())}

    private fun findDataFile(atDateTime: ZonedDateTime): Boolean {
        diskCapa()

        var lastRepoFile = ""
        var lastRepoSize = 0
        var lastRepoDate = TimeZero
        Path(REPO_ROOT).forEachDirectoryEntry {
            val thisRepoFile = it.fileName.toString()
            if ("Actim[0-9]{4}-[12][AB]_[-0-9_]{14,17}\\.csv".toRegex().matches(thisRepoFile)) {
                val thisRepoDate = thisRepoFile.parseFileDate()
                if (sensorName() == thisRepoFile.substring(0, 12) &&
                    (thisRepoDate <= atDateTime)) {
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
            fileHandle = BufferedWriter(FileWriter(file, true))
            fileHandle.append("\n")
            printLog("Continue data file $lastRepoFile", 10)
        }
        return false
    }

    private fun newDataFile(atDateTime: ZonedDateTime) {
        fileName = sensorName() + "_" + atDateTime.fileFormat() + ".csv"
        fileDate = atDateTime
        fileSize = 0
        val file = File(fileName.fullName())
        file.setWritable(true, false)
        fileHandle = BufferedWriter(FileWriter(file))
        fileHandle.append("\n")
        printLog("Start data file $fileName at $fileDate", 10)
    }

    fun countPoints(msgDateTime: ZonedDateTime, cycleNanoseconds: Long, count: Int): Int {
        var points = count
        if (lastDateTime != TimeZero) {
            points = Duration.between(lastDateTime, msgDateTime)
                .dividedBy(Duration.ofNanos(cycleNanoseconds))
                .toInt()
            lastDateTime += Duration.ofNanos(cycleNanoseconds * points)
        } else {
            lastDateTime = msgDateTime
        }
        return points
    }

    private fun writeData(dateTime: ZonedDateTime, textStr: String): Pair<Boolean, Int> {
        var newFile = false
        if (!this::fileHandle.isInitialized) newFile = findDataFile(dateTime)
        else if (fileSize > MAX_REPO_SIZE ||
            Duration.between(fileDate, dateTime) > MAX_REPO_TIME
        ) {
            fileHandle.close()
            diskCapa()
            newDataFile(dateTime)
            newFile = true
        }
        fileHandle.append(textStr + "\n")
        fileSize += textStr.length + 1
        return Pair(newFile, textStr.length + 1)
    }

    fun writeData(record: Record): Pair<Boolean, Int> {
        return writeData(record.dateTime, record.textStr)
    }

    fun writeData(record: RecordV3): Pair<Boolean, Int> {
        return writeData(record.dateTime, record.textStr)
    }

    fun closeIfOpen() {
        if (this::fileHandle.isInitialized) {
            try {
                fileHandle.close()
            } catch (e: Throwable) {
                printLog("Close file:$e", 10)
            }
            diskCapa()
        }
    }
}

