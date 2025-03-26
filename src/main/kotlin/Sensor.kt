@file:OptIn(ExperimentalUnsignedTypes::class)

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.io.path.*
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

class SignalData {
    var rawStr = ""

    fun read(buffer: UByte): SignalData {
        if (OUTPUT_RAW) {
            rawStr = "," +
                    if (buffer.toInt() and 0x01 != 0) "1"
                    else "0"
            rawStr += "," +
                    if (buffer.toInt() and 0x02 != 0) "1"
                    else "0"
            rawStr += "," +
                    if (buffer.toInt() and 0x04 != 0) "1"
                        else "0"
        }
        return this
    }
}

class Record(
    samplingMode: Int,
    hasSignals: Boolean,
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
        val signal = SignalData()
        var agSize = 6

        when (samplingMode) {
            2 -> gyro.read(buffer.sliceArray(0..5))
            0, 3 -> {
                accel.read(buffer.sliceArray(0..5))
                gyro.read(buffer.sliceArray(6..11))
                agSize = 12
            }
            else -> accel.read(buffer.sliceArray(0..5))
        }
        if (hasSignals) signal.read(buffer[agSize])

        textStr = dateTime.csvFormat() +
                ".%06d".format(dateTime.nano / 1_000L)
        if (OUTPUT_RAW) textStr += accel.rawStr + gyro.rawStr
        if (OUTPUT_VECTORS) textStr += accel.vecStr + gyro.vecStr
        if (OUTPUT_SIGNALS) textStr += signal.rawStr
    }
}

class SensorInfo(
    private val actimId: Int = 0,
    private val sensorId: String = "",
    private var fileName: String = "",
    private var fileSize: Int = 0,
    private var fileDate: ZonedDateTime = TimeZero
) {
    private lateinit var fileHandle: BufferedWriter
    private var lastDateTime: ZonedDateTime = TimeZero
    private val projectDir = Path("$REPO_ROOT/Project%02d".format(Projects[actimId] ?: 0))

    private fun sensorName(): String {
        return "Actim%04d-%s".format(actimId, sensorId.uppercase())
    }

    private fun findDataFile(atDateTime: ZonedDateTime) {
        var lastRepoFile = ""
        var lastRepoSize = 0
        var lastRepoDate = TimeZero
        if (projectDir.exists()) {
            projectDir.forEachDirectoryEntry {
                val thisRepoFile = it.fileName.toString()
                if ("Actim[0-9]{4}-[12][AB]_[-0-9_]{14,17}\\.csv".toRegex().matches(thisRepoFile)) {
                    val thisRepoDate = thisRepoFile.parseFileDate()
                    if (sensorName() == thisRepoFile.substring(0, 12) &&
                        (thisRepoDate <= atDateTime)
                    ) {
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
        } else {
            projectDir.createDirectory()
        }

        if (lastRepoFile == ""
            || (Duration.between(lastRepoDate, atDateTime) > MAX_REPO_TIME)
            || (lastRepoSize > MAX_REPO_SIZE)
        ) {
            newDataFile(atDateTime)
        } else {
            fileName = lastRepoFile
            fileDate = lastRepoDate
            fileSize = lastRepoSize
            val file = lastRepoFile.toFile(projectDir)
            file.setWritable(true, false)
            fileHandle = BufferedWriter(FileWriter(file, true))
            fileHandle.append("\n")
            printLog("Continue data file $projectDir/$fileName", 10)
        }
    }

    private fun newDataFile(atDateTime: ZonedDateTime) {
        fileName = sensorName() + "_" + atDateTime.fileFormat() + ".csv"
        fileDate = atDateTime
        fileSize = 0
        val file = fileName.toFile(projectDir)
        printLog("Create $file", 10)
        file.createNewFile()
        file.setWritable(true, false)
        fileHandle = BufferedWriter(FileWriter(file))
        fileHandle.append("\n")
        printLog("Start data file $projectDir/$fileName at $fileDate", 10)
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

    private fun writeData(dateTime: ZonedDateTime, textStr: String): Boolean {
        var newFile = false
        if (!this::fileHandle.isInitialized) {
            findDataFile(dateTime)
            newFile = true
        } else if (fileSize > MAX_REPO_SIZE ||
            Duration.between(fileDate, dateTime) > MAX_REPO_TIME
        ) {
            fileHandle.close()
            runSync(fileName.toFile(projectDir).toString(), false) { result: Int ->
                if (result == 42) Self.killActim(actimId)
                else printLog("Sync returned $result, nothing to do", 100)
            }
            newDataFile(dateTime)
            newFile = true
        }
        fileHandle.append(textStr + "\n")
        fileSize += textStr.length + 1
        return newFile
    }

    fun writeData(record: Record): Boolean {
        return writeData(record.dateTime, record.textStr)
    }

    fun closeIfOpen() {
        if (this::fileHandle.isInitialized) {
            try {
                fileHandle.close()
            } catch (e: Throwable) {
                printLog("Close file:$e", 10)
            }
        }
    }

    fun closeAndSync() {
        closeIfOpen()
        runSync(fileName.toFile(projectDir).toString(), true, null)
    }

    fun closeAndClear() {
        closeIfOpen()
        File(fileName.toFile(projectDir).toString()).delete()
    }
}

