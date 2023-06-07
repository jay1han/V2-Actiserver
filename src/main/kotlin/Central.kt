
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.BufferedReader
import java.io.DataOutputStream
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL

fun sendHttpRequest(reqString: String, data: String = ""): String {
    printLog(reqString + if (data != "") "\ndata=${data.cleanJson()}" else "")
    val centralURL = URL("HTTP", CENTRAL_HOST, HTTP_PORT, reqString)
    try {
        val connection = centralURL.openConnection() as HttpURLConnection
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
            responseText
        } else ""
        printLog("Response=${response.trim().cleanJson()}")
        return response
    } catch(e: Throwable) {
        printLog("httpRequest:$e")
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
                        "@${it.frequency}" + "(${it.sensorStr()})" + "%.3f%%".format(it.rating * 100.0)
                    }
                })
        val reqString = CENTRAL_BIN +
                "action=actiserver&serverId=${serverId}"
        val data = Json.encodeToString(Self.toCentral())
        val registryText = sendHttpRequest(reqString, data)
        if (registryText != "") {
            loadRegistry(registryText)
        }
    }
}
