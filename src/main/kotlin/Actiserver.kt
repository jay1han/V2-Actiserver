
import java.lang.Thread.sleep
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.ByteChannel
import java.nio.channels.ServerSocketChannel
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
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
        if (localRepo) options.isLocal = true
        if (options.isLocal) println("Repo is LOCAL")
        println("CENTRAL_HOST = $CENTRAL_HOST " +
                if (USE_HTTPS) "(HTTPS)" else ""
        )
    } else {
        println(netConfigOK)
        exitProcess(1)
    }

    while (! "/usr/bin/ntpstat".runCommand().contains("time correct")) {
        println("Waiting NTP sync")
        sleep(1000)
    }

    Self = Actiserver(serverId, myMachine, VERSION_STRING, myChannel, myIp, options.isLocal)
    selfToCentral()

    val actiServer = ServerSocketChannel.open().apply {
        configureBlocking(true)
        bind(InetSocketAddress(serverAddress, ACTI_PORT))
    }

    Thread.currentThread().priority = 6
    thread(start=true, name="mainloop", priority=8, isDaemon = true) {
        mainLoop()
    }
    thread(start=true, name="localserver", priority=8, isDaemon = true) {
        localServer()
    }
    thread(start=true, name="sideloop", priority=8, isDaemon = true) {
        sideLoop()
    }

    Runtime.getRuntime().addShutdownHook(
        thread (start=false) {
            println("Shutdown")
            printLog("Shutdown", 1)
            for (a in Self.actimetreList.values) {
                a.dies()
                a.thread.join()
            }
        })

    var clientCount = 0
    while (true) {
        println("Listening... $clientCount")
        val socket = actiServer.accept()
        clientCount += 1
        newClient(socket as ByteChannel)
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
            printLog("Actim%04d already known, close old channel".format(actimId), 1)
            Self.actimetreList[actimId]!!.restart()
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

fun localServer() {
    printLog("Local server", 1)
    val localServer = ServerSocketChannel.open().apply {
        configureBlocking(true)
        bind(InetSocketAddress(myIp, LOCAL_PORT))
    }

    while (true) {
        val client = localServer.accept()
    }
}

fun sideLoop() {
    printLog("Side Loop", 1)
    val localServer = ServerSocketChannel.open().apply {
        configureBlocking(true)
        bind(InetSocketAddress(serverAddress, SIDE_PORT))
    }

    while (true) {
        val actim = localServer.accept()

    }
}
