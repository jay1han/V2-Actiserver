@file:OptIn(ExperimentalUnsignedTypes::class)

import kotlinx.serialization.Required
import kotlinx.serialization.Serializable
import java.io.FileWriter
import java.nio.ByteBuffer
import java.nio.channels.ByteChannel
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.io.path.*

val Frequencies = listOf(100, 500, 1000, 2000, 4000, 8000)

@Serializable
class ActimetreShort(
    @Required var actimId           : Int = 9999,
    @Required private var mac       : String = "............",
    @Required private var boardType : String = "???",
    @Required private var version   : String = "000",
    @Required private var serverId  : Int = 0,
    @Required private var isDead    : Int = 0,
    @Required private var isStopped : Boolean = false,
    @Serializable(with = DateTimeAsString::class)
    @Required var bootTime          : ZonedDateTime = TimeZero,
    @Serializable(with = DateTimeAsString::class)
    @Required var lastSeen          : ZonedDateTime = TimeZero,
    @Serializable(with = DateTimeAsString::class)
    @Required var lastReport        : ZonedDateTime = TimeZero,
    @Required var sensorStr         : String = "",
    @Required var frequency         : Int = 0,
    @Required var rating            : Double = 1.0,
    @Required var rssi              : Int = 0,
    @Required var repoNums          : Int = 0,
    @Required var repoSize          : Long = 0,
) {
    fun init(a: Actimetre): ActimetreShort {
        actimId = a.actimId
        mac = a.mac
        boardType = a.boardType
        version = a.version
        serverId = a.serverId
        isDead = a.isDead
        isStopped = a.isStopped
        bootTime = a.bootTime
        lastSeen = a.lastSeen
        lastReport = a.lastReport
        sensorStr = a.sensorStr()
        frequency = a.frequency
        rating = a.rating
        rssi = a.rssi
        repoNums = a.repoNums
        repoSize = a.repoSize
        return this
    }
}

class Actimetre(
    val actimId   : Int = 0,
    var mac       : String = "",
    var boardType : String = "",
    var version   : String = "",
    val serverId  : Int = 0,
    var isDead    : Int = 0,
) {
    lateinit var thread: Thread
    var bootTime: ZonedDateTime = TimeZero
    var lastSeen: ZonedDateTime = TimeZero
    var lastReport: ZonedDateTime = TimeZero
    private var sensorList = mutableMapOf<String, SensorInfo>()
    private var nSensors = 0
    lateinit var channel: ByteChannel
    private var msgLength = 0
    private var sensorOrder = mutableListOf<String>()
    private var bootEpoch = 0L
    var frequency = 0
    private var cycleNanoseconds = 200000000L
    private var samplePoints  = 0
    private var totalPoints = 0
    var rating = 0.0
    var rssi: Int = 0
    var repoNums: Int = 0
    var repoSize: Long = 0
    private var htmlUpdate: ZonedDateTime = TimeZero
    var projectId   = 0
    private var projectPath = "Project%02d".format(Projects[actimId])
    private var projectDir  = Path("$REPO_ROOT/$projectPath")
    var isStopped = false

    private fun readInto(buffer: ByteBuffer): Int {
        var inputLen = 0
        val timeout = now().plusSeconds(1)
        try {
            while (inputLen < buffer.capacity() && now().isBefore(timeout)) {
                inputLen += this.channel.read(buffer)
            }
        } catch (e: Throwable) {
            printLog("${actimName()}:$e", 1)
            return 0
        }
        if (inputLen != buffer.capacity()) {
            printLog("${actimName()} sent $inputLen bytes < ${buffer.capacity()}", 1)
        }
        return inputLen
    }

    fun run(channel: ByteChannel) {
        this.channel = channel
        while (true) {
            val headerBuffer = ByteBuffer.allocate(HEADERV3_LENGTH)
            val inputLen = readInto(headerBuffer)
            if (inputLen != HEADERV3_LENGTH) {
                printLog("${actimName()} received header $inputLen bytes != $HEADERV3_LENGTH", 1)
                break
            }

            lastSeen = now()
            val sensorHeader = headerBuffer.array().toUByteArray()

            if ((sensorHeader[5].toInt() and 0x40) != 0) {
                printLog("${actimName()} Heartbeat", 1000)
                continue
            }

            if (sensorHeader[0].toInt() == 0xFF) {
                val messageLen = ((sensorHeader[3].toInt() and 0x3F) + 1) * 4
                val messageBuffer = ByteBuffer.allocate(messageLen)
                readInto(messageBuffer)
                val messageText = messageBuffer.array().decodeToString()
                printReport("[${actimName()}] $messageText")
                val reqString = CENTRAL_BIN + "action=actim-report&serverId=$serverId&actimId=$actimId"
                sendHttpRequest(reqString, "[${lastSeen.prettyFormat()}] $messageText")
                continue
            }

            val sensorName = "${'1' + ((sensorHeader[3].toInt() and 0x80) shr 7)}" +
                    "${(if ((sensorHeader[5].toInt() and 0x80) != 0) 'a' else 'A') +
                            ((sensorHeader[3].toInt() and 0x40) shr 6)}"
            val msgBootEpoch = sensorHeader.getInt3At(0).toLong()
            val msgMicros = sensorHeader.getInt3At20(5).toLong()
            val msgDateTime = ZonedDateTime.ofInstant(
                Instant.ofEpochSecond(bootEpoch + msgBootEpoch, msgMicros * 1000L),
                ZoneId.of("Z")
            )

            if (sensorHeader[5].toInt() and 0x10 != 0) {
                val messageLen = ((sensorHeader[3].toInt() and 0x3F) + 1) * 4
                val messageBuffer = ByteBuffer.allocate(messageLen)
                readInto(messageBuffer)
                val messageText = messageBuffer.array().decodeToString()
                printReport("[${actimName()}-$sensorName@${msgDateTime.microFormat()}] $messageText")
                val reqString = CENTRAL_BIN + "action=actim-report&serverId=$serverId&actimId=$actimId"
                sendHttpRequest(reqString, "[${msgDateTime.prettyFormat()}] $messageText")
                continue
            }

            if (!sensorList.containsKey(sensorName)) {
                printLog("${actimName()} undeclared sensor $sensorName", 1)
                printLog(sensorHeader.dump(), 10)
                break
            }
            val count = sensorHeader[3].toInt() and 0x3F

            rssi = (sensorHeader[4].toInt() shr 5) and 0x07
            val samplingMode = (sensorHeader[4].toInt() shr 3) and 0x03
            val hasSignals = (sensorHeader[5].toInt() and 0x20) != 0
            var dataLength = when (samplingMode) {
                0, 3 -> 12
                else -> 6
            }
            if (hasSignals) dataLength += 1

            val msgFrequency = sensorHeader[4].toInt() and 0x07
            if (msgFrequency >= Frequencies.size) {
                printLog("${actimName()} Frequency code $msgFrequency out of bounds", 1)
                printLog(sensorHeader.dump(), 10)
                break
            }
            if (frequency != Frequencies[msgFrequency]) {
                frequency = Frequencies[msgFrequency]
                cycleNanoseconds = 1_000_000_000L / frequency
                totalPoints = 0
                samplePoints = 0
                repoSize = 0
                repoNums = 0

                if (!projectDir.exists()) projectDir.createDirectory()
                projectDir.forEachDirectoryEntry("${actimName()}*") {
                    repoSize += it.fileSize()
                    repoNums++
                }
                printLog("${actimName()} repo $repoNums / $repoSize", 100)
            }

            if (count > 0) {
                val sensorBuffer = ByteBuffer.allocate(dataLength * count)
                val readLength = readInto(sensorBuffer)
                if (readLength != dataLength * count) {
                    printLog("${actimName()} Data length $readLength != ${dataLength * count}", 1)
                    break
                }

                if (!isStopped) {
                    val sensorData = sensorBuffer.array().toUByteArray()
                    for (index in 0 until count) {
                        val record = Record(
                            samplingMode,
                            hasSignals,
                            sensorData.sliceArray(index * dataLength until (index + 1) * dataLength),
                            bootEpoch, msgBootEpoch,
                            msgMicros - ((count - index - 1) * cycleNanoseconds / 1000L)
                        )
                        val newFile = sensorList[sensorName]!!.writeData(record)
                        htmlData(newFile)
                    }
                }
            } else {
                printLog("${actimName()}-$sensorName sent 0-data", 1000)
            }

            totalPoints += sensorList[sensorName]!!.countPoints(msgDateTime, cycleNanoseconds, count)
            samplePoints += count
            if (totalPoints > 0 && samplePoints <= totalPoints)
                rating = 1.0 - (samplePoints.toDouble() / totalPoints.toDouble())
            if (totalPoints > 60 * frequency && rating > 20.0) {
                printLog("${actimName()} unreliable, restarting", 1)
                break
            }
        }
    }

    private fun htmlData(force:Boolean = false) {
        if (force || lastSeen > htmlUpdate) {
            htmlUpdate = lastSeen.plusSeconds(60)

            printLog("Update ${actimName()}.html", 100)
            val repoList: MutableMap<String, MutableList<String>> = mutableMapOf()
            repoNums = 0
            repoSize = 0
            projectDir.forEachDirectoryEntry("${actimName()}*") {
                val fileDate = it.fileName.toString().parseFileDate().prettyFormat()
                val sensorStr = it.fileName.toString().substring(10, 12)
                val fileSize = try {
                    it.fileSize()
                } catch (_:Exception) { -1 }

                if (fileSize >= 0) {
                    repoNums++
                    repoSize += fileSize
                    if (repoList[sensorStr] == null) repoList[sensorStr] = mutableListOf()
                    repoList[sensorStr]!!.add(
                        """
                        <td>$fileDate</td><td>${fileSize.printSize()}</td>
                        <td><a href="/$projectPath/${it.fileName}">${it.fileName}</a></td>                
                        """.trimIndent()
                    )
                }
            }

            val htmlFile = FileWriter("index%04d.html".format(actimId).toFile(projectDir))
            htmlFile.write(
                """
                <html><head>
                <style>
                body {font-family:"Arial", "Helvetica", "Verdana", "Calibri", sans-serif; hyphens:manual;}
                table,th,tr,td {border:1px solid black; padding:0.3em; margin:0; border-collapse:collapse; text-align:center;}
                </style>
                <title>${actimName()} (${Self.serverName()})
                ${if (isStopped) "(Stopped)" else ""}
                </title></head><body>
                <h1>${actimName()} data on ${Self.serverName()}
                ${if (isStopped) "(Stopped)" else ""}
                </h1>
                <p>Files are locally stored on <b>${Self.serverName()}</b> (${Self.ip}) under $REPO_ROOT/$projectPath/</p>
                <p>$htmlInfo</p>
                <p>Right-click a file name and choose "Download link" to retrieve the file</p>
                <table><tr><th>Sensor</th><th>Date created</th><th>Size</th><th>File name</th></tr>
                """.trimIndent()
            )
            for (sensor in repoList.keys.sorted()) {
                val lines = repoList[sensor]!!.sorted()
                htmlFile.write("<tr><td rowspan=${lines.size}>$sensor</td>\n")
                htmlFile.write("${lines[0]}</tr>\n")
                for (line in lines.slice(1 until lines.size)) {
                    htmlFile.write("<tr>$line</tr>\n")
                }
            }
            htmlFile.write(
                """
                </table></body></html>
            """.trimIndent()
            )
            htmlFile.close()
        }
    }

    fun dies() {
        for (sensorInfo in sensorList.values) {
            sensorInfo.closeIfOpen()
        }
        if (this::channel.isInitialized) channel.close()
        printLog("${actimName()} dies", 1)
        if (isDead > 0) return
        isDead = 1
        frequency = 0
        val reqString = CENTRAL_BIN + "action=actimetre-off" +
                "&serverId=${serverId}&actimId=${actimId}"
        sendHttpRequest(reqString)
        htmlData(true)
        Self.rebootMaybe()
    }

    fun join() {
        if (this::thread.isInitialized) thread.join()
    }

    fun sendCommand(command: Int) {
        val commandBuffer = ByteBuffer.allocate(1)
        commandBuffer.array()[0] = command.toByte()
        try {
            channel.write(commandBuffer)
        } catch(e: Exception) {
            printLog("${actimName()} can't send command $command", 1)
        }
    }

    fun stop() {
        printLog("${actimName()} stopped", 1)
        isStopped = true
        for (sensorInfo in sensorList.values) {
            sensorInfo.closeAndSync()
        }
        htmlData(true)
    }

    fun stopStart() {
        printLog("${actimName()} starts stopped", 1)
        isStopped = true
        for (sensorInfo in sensorList.values) {
            sensorInfo.closeAndClear()
        }
        htmlData(true)
    }

    fun close() {
        for (sensorInfo in sensorList.values) {
            sensorInfo.closeIfOpen()
        }
        if (this::channel.isInitialized) channel.close()
        join()
    }

    fun cleanup() {
        if (isDead == 0 && !isStopped) {
            printLog("${actimName()} is not dead nor stopped", 1)
            return
        }
        if (!projectDir.exists()) {
            printLog("$projectPath doesn't exist",1)
            return
        }

        projectDir.forEachDirectoryEntry("${actimName()}*") {
            printLog("Sync ${it.fileName}")
            runSync(it.toAbsolutePath().toString(), true, null)
        }
        val htmlIndex = "index%04d.html".format(actimId).toFile(projectDir)
        if (htmlIndex.exists()) htmlIndex.delete()
        Self.removeActim(actimId)
        val reqString = CENTRAL_BIN + "action=actimetre-removed" +
                "&serverId=${serverId}&actimId=${actimId}"
        sendHttpRequest(reqString)
        printLog("${actimName()} removed", 1)
    }

    fun sensorStr(): String {
        var result = ""
        for (port in IntRange(1, 2)) {
            var portStr = "$port"
            for (address in listOf('A', 'a', 'B', 'b')) {
                if (sensorList.containsKey("$port$address")) {
                    portStr += "$address"
                }
            }
            if (portStr.length > 1) result += portStr
        }
        return result
    }

    fun loop(now: ZonedDateTime) {
        if (lastSeen == TimeZero) return
        if (isDead > 0 || !this::channel.isInitialized) {
            if (Duration.between(lastSeen, now) > SYNC_MINS) cleanup()
            return
        }
        if (Duration.between(bootTime, now) < ACTIM_BOOT_TIME) return
        if (Duration.between(lastSeen, now) > ACTIM_DEAD_TIME) {
            if (isDead > 0) return
            printLog(
                "${actimName()} last seen ${lastSeen.prettyFormat()}, " +
                        "${Duration.between(lastSeen, now).print()} before now ${now.prettyFormat()}",
                1)
            if (this::channel.isInitialized) channel.close()
            dies()
        }
        if (REBOOT_HOURS.toHours() > 0 && Duration.between(bootTime, now) > REBOOT_HOURS) {
            printLog(
                "${actimName()} booted ${bootTime.prettyFormat()}, rebooting now",
                10)
            dies()
        }
    }

    fun setInfo(mac: String, boardType: String, version: String, bootTime: ZonedDateTime, sensorBits: UByte) {
        this.mac = mac
        this.boardType = boardType
        this.version = version
        this.bootTime = bootTime
        lastSeen = bootTime
        lastReport = TimeZero
        totalPoints = 0
        samplePoints = 0
        rating = 0.0
        bootEpoch = bootTime.toEpochSecond()
        nSensors = 0
        for (port in 0..1) {
            for (address in 0..1) {
                val bitMask = 1 shl (port * 4 + address)
                val typeMask = 1 shl (port * 4 + address + 2)
                val sensorName = "%d%c".format(port + 1,
                    if ((sensorBits.toInt() and typeMask) != 0) 'a' + address
                    else 'A' + address)
                if ((sensorBits.toInt() and bitMask) != 0) {
                    sensorList[sensorName] = SensorInfo(actimId, sensorName)
                    nSensors += 1
                    sensorOrder.add(sensorName)
                }
            }
        }
        msgLength = nSensors * DATA_LENGTH + HEADER_LENGTH
        sensorOrder.sort()
        isDead = 0
    }

    fun setProject(projectId: Int) {
        this.projectId = projectId
        projectPath = "Project%02d".format(projectId)
        projectDir = Path("$REPO_ROOT/$projectPath")
        if (!projectDir.exists()) projectDir.createDirectory()
        printLog("${actimName()} project $projectId")
    }

    fun actimName(): String {
        return "Actim%04d".format(actimId)
    }
}

