
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.lang.Thread.sleep
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.ByteChannel
import java.nio.channels.ServerSocketChannel
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.Semaphore
import kotlin.concurrent.thread
import kotlin.io.path.Path
import kotlin.io.path.fileSize
import kotlin.io.path.forEachDirectoryEntry
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    Init()
    if (args.count() > 1) options = Options(args[1])

    println("Actiserver v$VERSION_STRING on $myMachine")
    println("$serverName WAN IP=$myIp, server $myIfname channel $myChannel IP=$serverAddress")

    if (netConfigOK == "") {
        printLog("$serverName started", 1)

        if (options.test) println("Test mode")
        println("REPO_ROOT = $REPO_ROOT. MAX_REPO_SIZE = $MAX_REPO_SIZE, MAX_REPO_TIME = $MAX_REPO_TIME")
        println("CENTRAL_HOST = $CENTRAL_HOST " +
                if (USE_HTTPS) "(HTTPS)" else ""
        )
    } else {
        println(netConfigOK)
        exitProcess(1)
    }

    while (! "/usr/bin/ntpstat".getTextOutput().contains("time correct")) {
        println("Waiting NTP sync")
        sleep(1000)
    }

    Self = Actiserver(serverId, myMachine, VERSION_STRING, myChannel, myIp)
    readRegistry()
    readProjects()
    selfToCentral()

    val actiServer = ServerSocketChannel.open().apply {
        configureBlocking(true)
        bind(InetSocketAddress(serverAddress, ACTI_PORT))
    }

    Thread.currentThread().priority = 6
    thread(start=true, name="mainloop", priority=8, isDaemon = true) {
        mainLoop()
    }
    thread(start=true, name="sideloop", priority=8, isDaemon = true) {
        sideLoop()
    }

    var shuttingDown = false
    Runtime.getRuntime().addShutdownHook(
        thread (start=false) {
            shuttingDown = true
            println("Shutdown")
            printLog("Shutdown initiated", 1)
            for (a in Self.actimetreList.values) {
                a.dies()
                a.join()
            }
            try {
                actiServer.close()
            } catch (e: Exception) {
                printLog(e.toString(), 1)
            }
            printLog("Shutdown complete", 1)
        })

    var clientCount = 0
    while (!shuttingDown) {
        println("Listening... $clientCount")
        try {
            val socket = actiServer.accept()
            if (shuttingDown) break
            clientCount += 1
            newClient(socket as ByteChannel)
        } catch (e: Throwable) {
            printLog("Actiserver:$e", 1)
        }
    }
}

fun newClient (channel: ByteChannel) {
    val messageBuffer = ByteBuffer.allocate(INIT_LENGTH)
    val inputLen: Int
    println("New client")

    try {
        inputLen = channel.read(messageBuffer)
        printLog("Init: read $inputLen", 1)
    } catch (e: Throwable) {
        printLog("newClient:$e", 1)
        return
    }

    if (inputLen != INIT_LENGTH) {
        printLog("Malformed first message, only $inputLen bytes", 1)
        return
    }

    val message = messageBuffer.array()
    val boardType = (message.slice(0..2)
        .map {it.toUByte().toInt().toChar()}).joinToString(separator="")
    val mac = (message.slice(3..8)
        .map {"%02X".format(it.toUByte().toInt())}).joinToString(separator="")
    val sensorBits = message[9].toUByte()
    val version = message.slice(10..12)
        .joinToString(separator="") {"%c".format(it)}
    val epochTime = now().toEpochSecond() + 1
    val bootTime = ZonedDateTime.ofInstant(
        Instant.ofEpochSecond(epochTime, 0),
        ZoneId.of("Z"))
    printLog("Actimetre MAC=$mac type $boardType version $version sensors " +
            sensorBits.parseSensorBits() +
            " booted at ${bootTime.prettyFormat()}", 1)

    val actimId = synchronized(Registry) {Registry[mac] ?: 0}
    var newActimId = actimId
    if (actimId == 0) {
        val reqString = CENTRAL_BIN +
                "action=actimetre-new&mac=${mac}&boardType=${boardType}" +
                "&version=${version}&serverId=${serverId}&bootTime=${bootTime.actiFormat()}"
        val response = sendHttpRequest(reqString, "")
        if (response != "") {
            val responseString = response.trim()
            newActimId = responseString.trim().toInt()
            synchronized(Registry) {
                Registry[mac] = newActimId
            }

            val isNew = if (responseString[0] == '+') {
                var fileNums = 0
                var fileSize = 0L
                try {
                    Path(REPO_ROOT).forEachDirectoryEntry("Actim%04d*".format(newActimId)) {
                        fileNums += 1
                        fileSize += it.fileSize()
                        it.toFile().delete()
                    }
                    printLog("Removed $fileNums files ($fileSize bytes) of old Actim%04d".format(newActimId), 1)
                } catch(e:Throwable) {
                    printLog("Clean:$e", 1)
                }
                " CLEAN"
            } else ""
            printLog("New Actim%04d for MAC=$mac".format(newActimId) + isNew, 1)
        } else {
            printLog("Received error from Acticentral, denying Actimetre", 1)
            return
        }
    } else {
        if (actimId in Self.actimetreList.keys) {
            printLog("Actim%04d already living, close".format(actimId), 1)
            val a = Self.actimetreList[actimId]!!
            Self.actimetreList.remove(actimId)
            a.close()
        } else {
            printLog("Returning Actim%04d".format(actimId), 1)
        }
    }

    val a = Self.updateActimetre(newActimId, mac, boardType, version, bootTime, sensorBits)
    printLog("${a.actimName()} type $boardType version $version sensors " +
            sensorBits.parseSensorBits() +
            " booted at ${bootTime.prettyFormat()}", 1)
    selfToCentral()

    val outputBuffer = ByteBuffer.allocate(6)
    val sentEpochTime = epochTime - 1
    outputBuffer.put(0, (newActimId shr 8).toByte())
    outputBuffer.put(1, (newActimId % 256).toByte())
    outputBuffer.put(2, (sentEpochTime shr 24).toByte())
    outputBuffer.put(3, ((sentEpochTime shr 16) and 0xFF).toByte())
    outputBuffer.put(4, ((sentEpochTime shr 8) and 0xFF).toByte())
    outputBuffer.put(5, (sentEpochTime and 0xFF).toByte())
    channel.write(outputBuffer)

    a.thread = thread(start=true, name="Actim%04d".format(newActimId), priority=4, isDaemon = false) {
        if (a.projectId == 0) {
            val commandBuffer = ByteBuffer.allocate(1)
            commandBuffer.array()[0] = 0x30
            channel.write(commandBuffer)
            a.stopStart()
            selfToCentral()
        }
        a.run(channel)
        a.dies()
        printLog("Cleaning up ${a.actimName()}", 1)
        selfToCentral()
        channel.close()
        println("${a.actimName()} Closed channel")
    }
}

fun mainLoop() {
    printLog("Main Loop", 1)
    var nextReport = now().plusSeconds(ACTIS_CHECK_SECS)
    var nextStatus = now()

    while (true) {
        val now = now()
        val actimList = synchronized(Self) {
            Self.actimetreList.values.toList()
        }
        for (a in actimList) {
            a.loop(now)
        }
        if (now.isAfter(nextStatus)) {
            globalStat()
            nextStatus = now.plusSeconds(ACTIS_STAT_SECS)
        }
        if (now.isAfter(nextReport)) {
            selfToCentral()
            nextReport = now.plusSeconds(ACTIS_CHECK_SECS)
        }
        sleep(1000L)
    }
}

@Serializable
data class Actis(
    val serverId: Int,
    val rssi: Int)

fun sideLoop() {
    printLog("Side Loop", 1)
    val localServer = ServerSocketChannel.open().apply {
        configureBlocking(true)
        bind(InetSocketAddress(serverAddress, SIDE_PORT))
    }

    while (true) {
        val channel = try {
            localServer.accept() as ByteChannel
        } catch (e:Throwable) {
            printLog("Side accept:$e", 1)
            continue
        }
        val messageBuffer = ByteBuffer.allocate(QUERY_LENGTH)
        val inputLen: Int
        println("New Actimetre")

        try {
            inputLen = channel.read(messageBuffer)
            printLog("Query: read $inputLen bytes", 100)
        } catch (e: Throwable) {
            printLog("Side read:$e", 1)
            continue
        }

        if (inputLen != QUERY_LENGTH) {
            printLog("Malformed query message, only $inputLen bytes", 1)
            return
        }

        val message = messageBuffer.array()
        val count = message[0].toInt()
        printLog("Query with $count items", 10)
        val actisList = arrayListOf<Actis>()
        for (i in 0 until count) {
            val serverId = message[1 + i * 3].toUByte().toInt() * 256 + message[2 + i * 3].toUByte().toInt()
            val rssi = message[3 + i * 3].toUByte().toInt()
            actisList.add(Actis(serverId, rssi))
            printLog("Actis$serverId: -${rssi}dB", 100)
        }

        val reqString = CENTRAL_BIN + "action=actimetre-query"
        val data = Json.encodeToString(actisList)
        printLog(data, 100)
        val assignedStr = sendHttpRequest(reqString, data)
        val assigned = try {
            assignedStr.toInt()
        } catch (e: NumberFormatException) {
            100
        }
        if (assigned < 100) {
            printLog("Assign $assigned: Actis%03d".format(actisList[assigned].serverId), 10)
        } else {
            printLog("Error assignment response: $assignedStr", 1)
        }

        val outputBuffer = ByteBuffer.allocate(1)
        outputBuffer.put(0, assigned.toByte())
        channel.write(outputBuffer)
        channel.close()
    }
}

data class SyncItem (
    val filename: String,
    val callback: ((Int) -> Unit)?
)

class SyncRunner() {
    val queue: MutableList<SyncItem> = mutableListOf()
    val semaphore = Semaphore(0)

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
