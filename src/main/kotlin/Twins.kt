import kotlinx.serialization.KSerializer
import kotlinx.serialization.Required
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encodeToString
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import java.time.Duration
import java.time.ZonedDateTime
import java.util.concurrent.Semaphore
import kotlin.concurrent.thread
import kotlin.io.path.*

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
    @Required var diskSize: Long = 0,
    @Required var diskFree: Long = 0,
    @Serializable(with = DateTimeAsString::class)
    @Required var lastReport : ZonedDateTime = TimeZero,
    @Serializable(with = DateTimeAsString::class)
    @Required var dbTime : ZonedDateTime = TimeZero,
    @Serializable(with = ActimetreShortList::class)
    @Required var actimetreList: Map<Int, ActimetreShort> = mapOf(),
) {
    @Required var isLocal: Boolean = true  // Compatibility
    @Required val isDown: Int = 0          // Compatibility
    @Required var cpuIdle: Float = 0.0f
    @Required var memAvail: Float = 0.0f
    @Required var diskTput: Float = 0.0f
    @Required var diskUtil: Float = 0.0f

    fun init(s: Actiserver) : ActiserverShort {
        serverId = s.serverId
        machine = s.machine
        version = s.version
        channel = s.channel
        ip = s.ip
        diskSize = s.diskSize
        diskFree = s.diskFree
        lastReport = s.lastReport
        dbTime = s.dbTime
        for (a in s.actimetreList.values) {
            a.lastReport = s.lastReport
        }
        actimetreList = s.actimetreList.map { Pair(it.key, ActimetreShort().init(it.value)) }.toMap()
        cpuIdle = s.stat.cpuIdle
        memAvail = s.stat.memAvailable
        diskTput = s.stat.diskThroughput
        diskUtil = s.stat.diskUtilization
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
    var diskSize: Long = 0
    var diskFree: Long = 0
    var lastReport: ZonedDateTime = TimeZero
    var dbTime: ZonedDateTime = TimeZero
    var actimetreList = mutableMapOf<Int, Actimetre>()
    private var hadAlive = false
    val stat = Stat()

    init {
        Path(REPO_ROOT).forEachDirectoryEntry("Project*") {
            it.forEachDirectoryEntry("Actim*") {
                val match = Regex("Actim([0-9]{4})-[12][ABab]_[-0-9_]{14,17}.csv").find(it.name)
                if (match != null) {
                    val actimId = match.groupValues[1].toInt()
                    val fileModified = it.getLastModifiedTime().toInstant()
                    if (Duration.between(fileModified, now()) > SYNC_MINS) {
                        runSync(it.toString(), true, null)
                    } else {
                        if (!actimetreList.containsKey(actimId)) {
                            actimetreList[actimId] = Actimetre(actimId, serverId = serverId, isDead = 1)
                        }
                        val a = actimetreList[actimId]!!
                        a.repoSize += it.fileSize()
                        a.repoNums ++
                        printLog("${it.name}: Actim%04d data ${a.repoNums} / ${a.repoSize}".format(actimId), 10)
                    }
                }
            }
        }
    }

    fun toCentral(): ActiserverShort {
        lastReport = now()
        return ActiserverShort().init(this)
    }

    fun df(size:Long, free:Long) {
        diskSize = size
        diskFree = free
    }

    fun updateActimetre(actimId: Int, mac: String, boardType: String, version: String,
                        bootTime: ZonedDateTime, sensorBits: UByte): Actimetre {
        synchronized(this) {
            if (!actimetreList.containsKey(actimId)) {
                actimetreList[actimId] = Actimetre(actimId, serverId = serverId)
            }
            val a = actimetreList[actimId]!!
            a.setInfo(mac, boardType, version, bootTime, sensorBits)
            a.isStopped = false
            a.setProject(Projects[actimId] ?: 0)
            hadAlive = true
            return a
        }
    }

    fun removeActim(actimId: Int) {
        synchronized(this) {
            actimetreList.remove(actimId)
        }
    }

    fun killActim(actimId: Int) {
        printLog("killActim $actimId")
        synchronized(this) {
            if (actimetreList.containsKey(actimId)) {
                val a = actimetreList[actimId]!!
                a.dies()
            } else {
                printLog("Unknown Actim" + "%04d".format(actimId))
            }
        }
    }

    fun serverName(): String {
        return "Actis%03d".format(serverId)
    }

    fun rebootMaybe() {
        if (shuttingDown) return
        val alive = actimetreList.countAlive()
        if (alive > 0)
            printLog("Remain $alive actimetres", 10)
        else if (hadAlive){
            printLog("No more actimetres", 10)
            val (result, text) = "/usr/bin/sudo /usr/sbin/reboot".runCommand()
            printLog("Reboot [$result]: $text", 10)
        } else {
            printLog("Never had actimetres", 10)
        }
    }
}

fun MutableMap<Int, Actimetre>.countAlive(): Int {
    var count = 0
    for (a in this.values) {
        if (a.isDead == 0) count++
    }
    return count
}

data class SyncItem (
    val filename: String,
    val callback: ((Int) -> Unit)?
)

class SyncRunner {
    private val queue: MutableList<SyncItem> = mutableListOf()
    private val semaphore = Semaphore(0)

    init {
        thread(name = "", isDaemon = true, start = true) {
            while (true) {
                semaphore.acquire()
                val filename = queue.first().filename
                val callback = queue.first().callback
                queue.removeFirst()

                val execString = SYNC_EXEC.replace("$", filename)
                printLog("SYNC: \"$execString\"", 10)
                val (result, text) = execString.runCommand()
                printLog("SYNC $filename\nreturned [$result]\n$text", 10)
                if (callback != null) callback(result)
                if (queue.size == 0)
                    printLog("SYNC queue is empty", 100)
                else
                    printLog("SYNC queue has ${queue.size} items", 100)
            }
        }
    }

    fun enqueue(
        filename: String,
        callback: ((Int) -> Unit)?
    ) {
        queue.add(SyncItem(filename, callback))
        printLog("SYNC enqueued $filename, ${queue.size} in total", 100)
        semaphore.release()
    }
}

val runner = SyncRunner()

fun runSync(
    filename: String,
    block: Boolean = false,
    callback: ((Int) -> Unit)?) {
    if (SYNC_EXEC == "") {
        printLog("SYNC_EXEC empty", 100)
    } else {
        if (block) {
            val execString = SYNC_EXEC.replace("$", filename)
            printLog("SYNC(block): \"$execString\"", 10)
            val (result, text) = execString.runCommand()
            printLog("SYNC(block) $filename\nreturned [$result] $text", 10)
            if (callback != null) callback(result)
        } else {
            runner.enqueue(filename, callback)
        }
    }
}
