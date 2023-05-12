@file:OptIn(ExperimentalUnsignedTypes::class)

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.ByteChannel
import java.nio.channels.ClosedChannelException
import java.nio.channels.ServerSocketChannel
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.system.exitProcess

@OptIn(ExperimentalUnsignedTypes::class)
fun main(args: Array<String>) {
    if (args.count() > 1) options = Options(args[1])

    var serverAddress = "192.168.${serverId}.1"

    if (options.test) {
        serverAddress = "192.168.1.50"
    }

    println("Welcome to Actiserver")
    println("mySsid is ${mySsid}, hence my serverId is $serverId")
    println("myIp is $myIp")
    println("myMac is $myMac")

    if (serverId > 0) {
        mqttClient = MQTTClient(4, CENTRAL_HOST, 1883, null, keepAlive = 0) {}
        mqttLog("$serverName started")

        if (options.echo) println("Echo on")
        if (options.logging) println("Logging on")
        if (options.test) println("Test mode")
        println("CENTRAL_HOST = $CENTRAL_HOST")
        println("UPLOAD_SIZE = $UPLOAD_SIZE; UPLOAD_TIME = $UPLOAD_TIME")
        println("MAX_REPO_SIZE = $MAX_REPO_SIZE; MAX_REPO_TIME = $MAX_REPO_TIME")
    } else {
        println("Unable to discover serverId, quitting")
        exitProcess(1)
    }

    uploadOrphans()
    loadSelf()
    selfToCentral()

    val actiServer = ServerSocketChannel.open().apply {
        configureBlocking(true)
        bind(InetSocketAddress(serverAddress, 2883))
    }

    runBlocking {
        launch {reportingLoop()}
        launch {mainLoop()}

        while (true) {
            println("Listening...")
            withContext(Dispatchers.IO) {
                val channel = actiServer.accept()
                launch {newClient(channel as ByteChannel)}
            }
        }
    }
}

suspend fun newClient (channel: ByteChannel) {
    val messageBuffer = ByteBuffer.allocate(14)
    val inputLen: Int
    println("New client")

    try {
        inputLen = channel.read(messageBuffer)
        printLog("Header: read $inputLen header: ${messageBuffer[0].toUInt()}")
    } catch (e: java.io.IOException) {
        printLog("IOException")
        channel.close()
        return
    } catch (e: ClosedChannelException) {
        printLog("ClosedChannelException")
        return
    }
    if (inputLen != 15) {
        printLog("Malformed first message, only $inputLen bytes")
        return
    }

    val message = messageBuffer.array()
    val boardType = (message.slice(0..2).map {it.toUByte().toInt().toChar()}).joinToString(separator="")
    val mac = (message.slice(3..8).map {"%02X".format(it.toUByte().toInt())}).joinToString(separator="")
    val bootTimeInt = message.getIntAt(9).toLong()
    val bootTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(bootTimeInt), ZoneId.of("Z"))
    printLog("Actimetre MAC=$mac type $boardType booted at ${bootTime.prettyFormat()}")

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
    val a = Self.updateActimetre(newActimId, mac, boardType, bootTime)
    dumpSelf()
    mqttLog("${a.actimName()} MAC=$mac type $boardType booted at ${bootTime.prettyFormat()}")

    val outputBuffer = ByteBuffer.allocate(2)
    outputBuffer.put(0, (newActimId shr 8).toByte())
    outputBuffer.put(1, (newActimId % 256).toByte())
    channel.write(outputBuffer)

    a.run(channel)
    a.dies()
    Self.removeActim(newActimId)
    dumpSelf()
}

suspend fun mainLoop() {
    printLog("Main Loop")
    while(true) {
        val now = now()
        for (a in Self.actimetreList.values) {
            a.loop(now)
        }
        delay(1000)
    }
}

suspend fun reportingLoop() {
    printLog("Reporting Loop")
    while(true) {
        delay(ACTIS_CHECK_MILLIS)
        selfToCentral()
    }
}
