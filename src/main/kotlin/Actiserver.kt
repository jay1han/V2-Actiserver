@file:OptIn(ExperimentalUnsignedTypes::class)

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.ByteChannel
import java.nio.channels.ClosedChannelException
import java.nio.channels.ServerSocketChannel
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.concurrent.fixedRateTimer
import kotlin.concurrent.thread
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    if (args.count() > 1) options = Options(args[1])

    var serverAddress = "192.168.${serverId}.1"

    if (options.test) {
        serverAddress = myIp
    }

    println("Welcome to Actiserver")
    println("mySsid=${mySsid}, serverId=$serverId, serverAddress=$serverAddress")
    println("myMac=$myMac, myIp=$myIp, myChannel=$myChannel")

    if (serverId > 0) {
        mqttClient = MQTTClient(4, CENTRAL_HOST, MQTT_PORT, null, keepAlive = 0) {}
        mqttLog("$serverName started")

        if (options.echo) println("Echo on")
        if (options.logging) println("Logging on")
        if (options.test) println("Test mode")
        if (options.fullText) println("Full text")
        println("CENTRAL_HOST = $CENTRAL_HOST, ACTI_PORT=$ACTI_PORT, MQTT_PORT=$MQTT_PORT")
        println("MAX_REPO_SIZE = $MAX_REPO_SIZE; MAX_REPO_TIME = $MAX_REPO_TIME")
    } else {
        println("Unable to discover serverId, quitting")
        exitProcess(1)
    }

    Self = Actiserver(serverId, myMac, myIp, myChannel, now())
    selfToCentral()

    val actiServer = ServerSocketChannel.open().apply {
        configureBlocking(true)
        bind(InetSocketAddress(serverAddress, ACTI_PORT))
    }

    Thread.currentThread().priority = 6
    thread(start=true, isDaemon = true, name="reporting", priority=2) {reportingLoop()}
    thread(start=true, isDaemon = true, name="loop", priority=9) {mainLoop()}

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
    val epochTime = now().toEpochSecond() + 1
    val bootTime = ZonedDateTime.ofInstant(
        Instant.ofEpochSecond(epochTime, 0),
        ZoneId.of("Z"))
    printLog("Actimetre MAC=$mac type $boardType sensors %02X booted at ${bootTime.prettyFormat()}".format(sensorBits))

    val actimId = Registry[mac] ?: 0
    var newActimId = actimId
    if (actimId == 0) {
        val reqString = CENTRAL_BIN +
                "action=actimetre-new&mac=${mac}&boardType=${boardType}&serverId=${serverId}&bootTime=${bootTime.actiFormat()}"
        val responseString = sendHttpRequest(reqString, "")
        newActimId = responseString.trim().toInt()
        Registry[mac] = newActimId
        printLog("New Actim%04d".format(newActimId))

    } else {
        printLog("Known Actim%04d".format(actimId))
    }

    val a = Self.updateActimetre(newActimId, mac, boardType, bootTime, sensorBits)
    mqttLog("${a.actimName()} MAC=$mac type $boardType sensors %02X booted at ${bootTime.prettyFormat()}".format(sensorBits))
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
    while (true) {
        val now = now()
        val actimList = Self.actimetreList.values.toList()
        for (a in actimList) {
            a.loop(now)
        }
        Thread.sleep(500L)
    }
}

fun reportingLoop() {
    printLog("Reporting Loop")
    fixedRateTimer("Reporting", true, ACTIS_CHECK_MILLIS, ACTIS_CHECK_MILLIS) {
        selfToCentral()
        Thread.yield()
    }
}
