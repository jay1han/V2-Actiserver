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


private fun makeInt(msb: UByte, lsb: UByte) : Int {
    var integer = msb.toInt() * 256 + lsb.toInt()
    if (integer >= 32768) integer -= 65536
    return integer
}

private fun makeAccelStr(buffer: UByteArray): String{
    val rawX = makeInt(buffer[0], buffer[1])
    val rawY = makeInt(buffer[2], buffer[3])
    val rawZ = makeInt(buffer[4], buffer[5])
    return arrayOf(
        rawX / 8192.0f,
        rawY / 8192.0f,
        rawZ / 8192.0f
    ).joinToString(separator = ",") { "%+07.4f".format(it) }
}

private fun makeGyroStr(buffer: UByteArray): String {
    val rawX = makeInt(buffer[0], buffer[1])
    val rawY = makeInt(buffer[2], buffer[3])
    return arrayOf(
        rawX / 131.0f,
        rawY / 131.0f
    ).joinToString(separator = ",") { "%+07.3f".format(it) }
}

class Record(buffer: UByteArray, val sensorId: String, bootEpoch: Long, msgBootEpoch: Long, msgMillis: Long) {
    private val diffMillis = buffer[0].toLong() * 256 + buffer[1].toLong()
    val dateTime: ZonedDateTime = ZonedDateTime.ofInstant(
        Instant.ofEpochSecond(bootEpoch + msgBootEpoch,
            (msgMillis + diffMillis) * 1_000_000L),
        ZoneId.of("Z"))
    private val accelStr = makeAccelStr(buffer.sliceArray(2..7))
    private val gyroStr = makeGyroStr(buffer.sliceArray(8..11))
    val textStr: String = dateTime.csvFormat() +
            ".%03d,".format(dateTime.nano / 1000000L) +
            accelStr + "," + gyroStr
}

class RecordV3(samplingMode: Int, buffer: UByteArray, bootEpoch: Long, msgBootEpoch: Long, msgMicros: Long) {
    val dateTime: ZonedDateTime = ZonedDateTime.ofInstant(
        Instant.ofEpochSecond(bootEpoch + msgBootEpoch,
            msgMicros * 1_000L),
        ZoneId.of("Z"))

    var textStr: String

    init {
        var accelStr = "0,0,0"
        var gyroStr = "0,0"

        when (samplingMode) {
            1 -> accelStr = makeAccelStr(buffer.sliceArray(0..5))
            2 -> gyroStr = makeGyroStr(buffer.sliceArray(0..3))
            else -> {
                accelStr = makeAccelStr(buffer.sliceArray(0..5))
                gyroStr = makeGyroStr(buffer.sliceArray(6..9))
            }
        }
        textStr = dateTime.csvFormat() +
                ".%06d,".format(dateTime.nano / 1_000L) +
                accelStr + "," + gyroStr
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
            if ("Actim[0-9]{4}-[12][AB]_[0-9]{14}\\.csv".toRegex().matches(thisRepoFile)) {
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
            printLog("Continue data file $lastRepoFile")
        }
        return false
    }

    private fun newDataFile(atDateTime: ZonedDateTime) {
        fileName = sensorName() + "_" + atDateTime.actiFormat() + ".csv"
        fileDate = atDateTime
        fileSize = 0
        val file = File(fileName.fullName())
        file.setWritable(true, false)
        fileHandle = BufferedWriter(FileWriter(file))
        fileHandle.append("\n")
        printLog("Start data file $fileName at $fileDate")
    }

    fun writeData(dateTime: ZonedDateTime, textStr: String): Pair<Boolean, Int> {
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
                printLog("Close file:$e")
            }
            diskCapa()
        }
    }
}

