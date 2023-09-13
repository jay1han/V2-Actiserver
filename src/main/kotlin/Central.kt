
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.BufferedReader
import java.io.DataOutputStream
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL
import javax.net.ssl.HttpsURLConnection

fun sendHttpRequest(reqString: String, data: String = ""): String {
    printLog(reqString + if (data != "") "\ndata=${data.cleanJson()}" else "")
    var centralURL: URL
    if (USE_HTTPS) {
        centralURL = URL("https://$CENTRAL_HOST$reqString&secret=$SECRET_KEY")
    } else {
        centralURL = URL("http://$CENTRAL_HOST$reqString")
    }
    printLog(centralURL.toString())

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
            responseText
        } else ""
        printLog("Response[$responseCode]:${response.trim()}")
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
