
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.BufferedReader
import java.io.DataOutputStream
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL
import javax.net.ssl.HttpsURLConnection

fun sendHttpRequest(reqString: String, data: String = ""): String {
    printLog(reqString, 10)
    if (data != "") printLog("\ndata=${data.cleanJson()}", 100)
    val centralURL =
        if (USE_HTTPS) URL("https://$CENTRAL_HOST$reqString&secret=$SECRET_KEY")
        else URL("http://$CENTRAL_HOST$reqString&secret=$SECRET_KEY")

    printLog(centralURL.toString(), 100)
    printLog(data, 100)

    try {
        val URLconnection = centralURL.openConnection()
        var connection = URLconnection as HttpURLConnection
        if (USE_HTTPS) {
            connection = URLconnection as HttpsURLConnection
        }

        connection.doInput = true
        if (data != "") {
            connection.requestMethod = "POST"
            connection.doOutput = true
            connection.setRequestProperty("Content-Length", data.length.toString())
            DataOutputStream(connection.outputStream).use { it.writeBytes(data) }
        } else {
            connection.requestMethod = "GET"
        }
        val responseCode = connection.responseCode
        val response = if (responseCode == 200) {
            val input = connection.inputStream
            val reader = BufferedReader(InputStreamReader(input))
            val responseText = reader.readText()
            input.close()
            responseText.trim()
        } else ""
        printLog("Response[$responseCode]:${if (response.length < 40) response else "[${response.length}]"}", 10)
        return response
    } catch(e: Throwable) {
        printLog("httpRequest:$e", 1)
    }
    return ""
}

fun selfToCentral() {
    synchronized(Self) {
        Self.lastReport = now()
        printLog("v${VERSION_STRING} Alive with " +
                if (Self.actimetreList.isEmpty()) "no Actimetres"
                else Self.actimetreList.keys.sorted().joinToString(separator = " ") {
                    "Actim%04d".format(it) + Self.actimetreList[it]?.let {
                        "[${it.projectId}]@${it.frequency}" + "(${it.sensorStr()})" +
                                if (it.isDead > 0) "Dead"
                                else if (it.isStopped) "Stopped"
                                else "%.3f%%".format(it.rating * 100.0)
                    }
                }, 1
        )
        val reqString = CENTRAL_BIN + "action=actiserver3&serverId=$serverId"
        val data = Json.encodeToString(Self.toCentral())
        val responseText = sendHttpRequest(reqString, data)
        for (line in responseText.lines()) {
            if (line.startsWith("+")) {
                val actimId = line.substring(1).substringBefore(':').toInt()
                val command = line.substring(1).substringAfter(':').toInt()
                if (actimId in Self.actimetreList.keys) {
                    when (command) {
                        0x10, 0xF0 -> {
                            printLog("Send command ${"0x%02X".format(command)} to Actimetre $actimId", 1)
                            val actim = Self.actimetreList[actimId]!!
                            actim.sendCommand(command)
                        }

                        0x20 -> {
                            printLog("Clean up Actim%04d data".format(actimId), 1)
                            val actim = Self.actimetreList[actimId]!!
                            if (actim.isDead == 0) {
                                printLog("${actim.actimName()} is not dead!", 1)
                            } else {
                                actim.cleanup()
                                selfToCentral()
                            }
                        }

                        0x30 -> {
                            printLog("Stop Actimetre $actimId", 1)
                            val actim = Self.actimetreList[actimId]!!
                            actim.sendCommand(command)
                            actim.stop()
                            selfToCentral()
                        }

                        else -> {
                            printLog("Unknown command $command for Actimetre $actimId", 1)
                        }
                    }
                } else {
                    printLog("No Actimetre $actimId to apply $command to", 1)
                }
            } else if (line.startsWith("!")) {
                Self.dbTime = now()
                fetchRegistry()
                fetchProjects()
            }
        }
    }
}

fun fetchRegistry() {
    printLog("Fetch registry", 1)
    val reqString = CENTRAL_BIN + "action=registry&serverId=$serverId"
    val responseText = sendHttpRequest(reqString)
    loadRegistry(responseText)
    saveRegistry()
}

fun fetchProjects() {
    printLog("Fetch projects", 1)
    val reqString = CENTRAL_BIN + "action=projects&serverId=$serverId"
    val responseText = sendHttpRequest(reqString)
    loadProjects(responseText)
    saveProjects()
}
