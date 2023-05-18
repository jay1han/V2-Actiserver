
import kotlinx.serialization.KSerializer
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import java.io.FileWriter
import java.io.Writer
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.time.*
import java.time.format.DateTimeFormatter
import kotlin.concurrent.thread
import kotlin.io.path.*

fun uploadOrphans() {
    Path(DATA_ROOT).forEachDirectoryEntry {
        uploadFile(null, it.fileName.toString())
    }
}

fun uploadFile(fileHandle: Writer?, fileName: String) =
    thread(start = true, isDaemon = true, name = "upload", priority = 2) {
        fileHandle?.close()
        val sensorName = fileName.substring(0, 12)
        var lastRepoFile = ""
        var lastRepoSize = 0
        var lastRepoDate = TimeZero
        Path(REPO_ROOT).forEachDirectoryEntry {
            val thisRepoFile = it.fileName.toString()
            val thisRepoDate = thisRepoFile.parseFileDate()
            if (sensorName == thisRepoFile.substring(0, 12)) {
                if (lastRepoFile == "" ||
                    (Duration.between(lastRepoDate, thisRepoDate) > Duration.ofSeconds(0))
                ) {
                    lastRepoFile = thisRepoFile
                    lastRepoSize = it.fileSize().toInt()
                    lastRepoDate = thisRepoDate
                }
            }
        }

        if (lastRepoFile == "" ||
            (Duration.between(lastRepoDate, fileName.parseFileDate()) > MAX_REPO_TIME) ||
            (lastRepoSize > MAX_REPO_SIZE)) {
            Path(fileName.DATAname()).moveTo(Path(fileName.REPOname()), overwrite = true)
            mqttLog("Repo $fileName")
        } else {
            appendFile(fileName.DATAname(), lastRepoFile.REPOname())
            mqttLog("Repo $lastRepoFile")
        }
    }

fun appendFile(inFileName: String, outFileName: String) {
    val infile = Files.newBufferedReader(Path(inFileName))
    val outfile =
        Files.newBufferedWriter(Path(outFileName), StandardOpenOption.APPEND, StandardOpenOption.WRITE)
    outfile.append(infile.readText())
    infile.close()
    outfile.close()
    Path(inFileName).deleteExisting()
}

var Registry = mutableMapOf<String, Int>()

fun loadRegistry(registryText: String) {
    Registry = Json.decodeFromString<MutableMap<String,Int>>(registryText)
}

fun String.DATAname(): String {return "$DATA_ROOT/$this" }
fun String.REPOname(): String {return "$REPO_ROOT/$this" }

private val actiFormat  : DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
private val prettyFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
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

fun ZonedDateTime.actiFormat(): String {
    return this.format(actiFormat)
}

fun String.parseActiFormat(): ZonedDateTime {
    return ZonedDateTime.of(LocalDateTime.parse(this, actiFormat), ZoneId.of("Z"))
}

fun String.parseFileDate(): ZonedDateTime {
    return this.substring(13,27).parseActiFormat()
}

fun ByteArray.getInt3At(index: Int): Int {
    return this[index].toUByte().toInt() * (1 shl 16) + this[index + 1].toUByte().toInt() * (1 shl 8) +
            this[index + 2].toUByte().toInt()
}
