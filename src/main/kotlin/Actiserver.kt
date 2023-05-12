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
        printLog("Actis$serverId started")
        if (options.echo) println("Echo on")
        if (options.logging) println("Logging on")
        if (options.test) println("Test mode")

        println("CENTRAL_HOST = $CENTRAL_HOST")
        mqttClient = MQTTClient(4, CENTRAL_HOST, 1883, null, keepAlive = 0) {}
        println("UPLOAD_SIZE = $UPLOAD_SIZE; UPLOAD_TIME = $UPLOAD_TIME")
        println("MAX_REPO_SIZE = $MAX_REPO_SIZE; MAX_REPO_TIME = $MAX_REPO_TIME")
    }

    uploadOrphans()
    loadSelf()
    mqttLog("${myMac}($myIp) started")
    selfToCentral()

    val actiServer = ServerSocketChannel.open().apply {
        configureBlocking(true)
        bind(InetSocketAddress(serverAddress, 2883))
    }

    runBlocking {
        launch {reportingLoop()}
        launch {mainLoop()}

        while (true) {
            printLog("Listening...")
            withContext(Dispatchers.IO) {
                val channel = actiServer.accept()
                launch {newClient(channel as ByteChannel)}
            }
        }
    }
}

suspend fun newClient (channel: ByteChannel) {
    val header = ByteBuffer.allocate(1)
    var looping = true
    var actimId = 0

    printLog("New client")
    while (looping) {
        var inputLen = 0
        header.position(0)

        try {
            inputLen = channel.read(header)
            printLog("Header: read $inputLen bytes: ${header[0].toUInt()}")
        } catch (e: java.io.IOException) {
            printLog("IOException")
            break
        } catch (e: ClosedChannelException) {
            printLog("ClosedChannelException")
            break
        }
        if (inputLen != 1) break

        // Let's manage the communication fronm within the Actimetre class
        val actimState =
            if (header[0].toUInt() == 0x40u || header[0].toUInt() == 0x20u) {
                actimAlive(header[0].toUByte().toInt(), channel)
            } else if (actimId > 0) {
                actimData(actimId, header[0].toUByte().toInt(), channel)
            } else ActimResult(false, 0)
        if (actimState.alive) actimId = actimState.actimId
        looping = actimState.alive
        printLog("actimState ${actimState.alive} ${actimState.actimId}")
    }
    actimDies(actimId)
    printLog("Closing $actimId")
}

data class ActimResult (
    val alive: Boolean,
    val actimId: Int
)

// *AA tttmmmmmmbbbbbbbbbbbbbb [2+23]
// First byte 0x40 bit 1
suspend fun actimAlive(code: Int, channel: ByteChannel): ActimResult {
    printLog("actimAlive")
    val bodyBuffer = ByteBuffer.allocate(13)
    val body = bodyBuffer.array()

    var inputLen = 0
    while (inputLen < 13) {
        inputLen += channel.read(bodyBuffer)
        printLog("Info: read $inputLen bytes")
    }
    if (inputLen != 13) return ActimResult(false, 0)

    val boardType = (body.slice(0..2).map {it.toUByte().toInt().toChar()}).joinToString(separator="")
    val mac = (body.slice(3..8).map {"%02X".format(it.toUByte().toInt())}).joinToString(separator="")
    val bootTimeInt = body.getIntAt(9).toLong()
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
        printLog("New actimetre $newActimId")
    } else {
        printLog("Known actimetre $actimId")
    }

    if (code == 0x40) {
        printLog("Responding")
        val outputBuffer = ByteBuffer.allocate(2)
        outputBuffer.put(0, (newActimId shr 8).toByte())
        outputBuffer.put(1, (newActimId % 256).toByte())
        channel.write(outputBuffer)
    }

    Self.updateActimetre(newActimId, mac, boardType, bootTime)
    mqttLog("Actim${newActimId} is $boardType:${mac} started ${bootTime.prettyFormat()}")
    return ActimResult(true, newActimId)
}

// AAn(Pssssuuuuaaaaaagggg)+
// First byte 0x40 bit 0
suspend fun actimData(actimId: Int, nSensors: Int, channel: ByteChannel): ActimResult {
    printLog("actimData $actimId for $nSensors sensors")

    for (i in 1..nSensors) {
        val bodyBuffer = ByteBuffer.allocate(18)
        var inputLen = 0
        while (inputLen < 18) {
            inputLen += channel.read(bodyBuffer)
            printLog("Sensor $i: read $inputLen bytes")
        }
        if (inputLen != 18) {
            return ActimResult(false, actimId)
        }

        val record = Record(bodyBuffer.array())
        printLog("Record for Actim$actimId sensorId=${record.sensorId}: ${record.textStr}")
        printLog("Actim$actimId is " + Self.actimetreList[actimId].toString())
        Self.actimetreList[actimId]?.addRecord(record)
    }
    return ActimResult(true, actimId)
}

suspend fun mainLoop() {
    printLog("Main Loop")
    while(true) {
        val now = now()
        for (a in Self.actimetreList.values) {
            when(a.loopOk(now)) {
                ActimState.Dead -> actimDies(a.actimId)
                ActimState.MustReport -> actimReport(a, now)
                else -> {}
            }
        }
        delay(1000)
    }
}

suspend fun reportingLoop() {
    printLog("reportingLoop")
    while(true) {
        delay(ACTIS_CHECK_MILLIS)
        selfToCentral()
    }
}

suspend fun actimReport(a: Actimetre, now: ZonedDateTime) {
    val reqString = CENTRAL_BIN + "action=actimetre" +
            "&serverId=${serverId}&actimId=${a.actimId}&sensorStr=${a.sensorStr()}"
    sendHttpRequest(reqString, Json.encodeToString(value = a))
    mqttLog("${a.actimName()} reported")
    a.lastReport = now
}

suspend fun actimDies(actimId: Int) {
    printLog("Actim%04d dies".format(actimId))
    val reqString = CENTRAL_BIN + "action=actimetre-off" +
            "&serverId=${serverId}&actimId=${actimId}"
    sendHttpRequest(reqString)

    var removed: Actimetre?
    Self.mutex.withLock {
        removed = Self.actimetreList.remove(actimId)
    }
    if (removed != null) {
        mqttLog("${removed!!.actimName()} died")
        dumpSelf()
    }
}
