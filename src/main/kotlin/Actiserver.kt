
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.ByteChannel
import java.nio.channels.ClosedChannelException
import java.nio.channels.ServerSocketChannel
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.concurrent.thread
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    if (args.count() > 1) options = Options(args[1])

    println("Actiserver v$VERSION_STRING on $myMachine")
    println("$serverName at $myIp device $wlan on channel $myChannel as $serverAddress")

    if (serverId > 0) {
        mqttLog("$serverName started")

        if (options.echo) println("Echo on")
        if (!options.logging) println("Logging off")
        if (options.test) println("Test mode")
        println("REPO_ROOT = $REPO_ROOT. MAX_REPO_SIZE = $MAX_REPO_SIZE, MAX_REPO_TIME = $MAX_REPO_TIME")
        println("CENTRAL_HOST = $CENTRAL_HOST. MQTT_HOST = $MQTT_HOST")
    } else {
        println("Unable to discover serverId, quitting")
        exitProcess(1)
    }

    Self = Actiserver(serverId, myMachine, VERSION_STRING, myChannel, myIp)
    selfToCentral()

    val actiServer = ServerSocketChannel.open().apply {
        configureBlocking(true)
        bind(InetSocketAddress(serverAddress, ACTI_PORT))
    }

    Thread.currentThread().priority = 6
    thread(start=true, name="loop", priority=8, isDaemon = true) {mainLoop()}

    var clientCount = 0
    while (true) {
        println("Listening... $clientCount")
        val channel = actiServer.accept() as ByteChannel
        clientCount += 1
        thread(start=true, name="$clientCount", priority=4) {
            newClient(channel)
            channel.close()
            println("Closed channel")
        }
    }
}

fun newClient (channel: ByteChannel) {
    val messageBuffer = ByteBuffer.allocate(INIT_LENGTH)
    val inputLen: Int
    println("New client")

    try {
        inputLen = channel.read(messageBuffer)
        printLog("Init: read $inputLen")
    } catch (e: java.io.IOException) {
        printLog("IOException")
        return
    } catch (e: ClosedChannelException) {
        printLog("ClosedChannelException")
        return
    }
    if (inputLen != INIT_LENGTH) {
        printLog("Malformed first message, only $inputLen bytes")
        return
    }

    val message = messageBuffer.array()
    val boardType = (message.slice(0..2).map {it.toUByte().toInt().toChar()}).joinToString(separator="")
    val mac = (message.slice(3..8).map {"%02X".format(it.toUByte().toInt())}).joinToString(separator="")
    val sensorBits = message[9]
    val version = message.slice(10..12).joinToString(separator="") {"%c".format(it)}
    val epochTime = now().toEpochSecond() + 1
    val bootTime = ZonedDateTime.ofInstant(
        Instant.ofEpochSecond(epochTime, 0),
        ZoneId.of("Z"))
    printLog("Actimetre MAC=$mac type $boardType version $version sensors %02X booted at ${bootTime.prettyFormat()}".format(sensorBits))

    val actimId = Registry[mac] ?: 0
    var newActimId = actimId
    if (actimId == 0) {
        val reqString = CENTRAL_BIN +
                "action=actimetre-new&mac=${mac}&boardType=${boardType}&version=${version}&serverId=${serverId}&bootTime=${bootTime.actiFormat()}"
        val responseString = sendHttpRequest(reqString, "")
        if (responseString != "") {
            newActimId = responseString.trim().toInt()
            Registry[mac] = newActimId
            printLog("New Actim%04d".format(newActimId))
        } else {
            printLog("Received error from Acticentral, denying Actimetre")
            return
        }
    } else {
        printLog("Known Actim%04d".format(actimId))
    }

    val a = Self.updateActimetre(newActimId, mac, boardType, version, bootTime, sensorBits)
    mqttLog("${a.actimName()} MAC=$mac type $boardType version $version sensors %02X booted at ${bootTime.prettyFormat()}".format(sensorBits))
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

    a.run(channel)

    a.dies()
    mqttLog("Cleaning up ${a.actimName()}")
    selfToCentral()
}

fun mainLoop() {
    printLog("Main Loop")
    var nextReport = now().plusSeconds(ACTIS_CHECK_SECS)
    while (true) {
        val now = now()
        val actimList = Self.actimetreList.values.toList()
        for (a in actimList) {
            a.loop(now)
        }
        if (now().isAfter(nextReport)) {
            selfToCentral()
            nextReport = now().plusSeconds(ACTIS_CHECK_SECS)
        }
        Thread.sleep(1000L)
    }
}
