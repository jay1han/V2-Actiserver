@file:OptIn(ExperimentalUnsignedTypes::class)

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import mqtt.packets.Qos
import java.io.BufferedReader
import java.io.DataOutputStream
import java.io.IOException
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL

fun sendHttpRequest(reqString: String, data: String = ""): String {
    printLog(reqString + if (data != "") ", data=$data" else "")
    val centralURL = URL("HTTP", CENTRAL_HOST, HTTP_PORT, reqString)
    val connection = centralURL.openConnection() as HttpURLConnection
    connection.doInput = true
    if (data != "") {
        connection.requestMethod = "POST"
        connection.doOutput = true
        connection.setRequestProperty("Content-Length", data.length.toString())
        DataOutputStream(connection.outputStream).use {it.writeBytes(data)}
    } else {
        connection.requestMethod = "GET"
    }
    val responseCode = connection.responseCode
    val response = if (responseCode == 200) {
        val input = connection.inputStream
        val reader = BufferedReader(InputStreamReader(input))
        val responseText = reader.readText()
        input.close()
        return responseText
    } else "Error $responseCode from Acticentral"
    printLog("Response=$response")
    return response
}

fun selfToCentral() {
    synchronized(Self) {
        Self.lastReport = now()
        mqttLog("v${VERSION_STRING} Alive with " +
                if (Self.actimetreList.count() == 0) "no Actimetres"
                else Self.actimetreList.keys.sorted().joinToString(separator = " ") {
                    "Actim%04d".format(it)
                })
        val reqString = CENTRAL_BIN +
                "action=actiserver&serverId=${serverId}&ip=${myIp}&mac=${myMac}&version=${VERSION_STRING}&machine=$myMachine"
        val data = Json.encodeToString(Self.toCentral())
        val registryText = sendHttpRequest(reqString, data)
        loadRegistry(registryText)
        printLog("Registry: " + Registry.toString())
    }
}

lateinit var mqttClient: MQTTClient

fun mqttLog(text: String) {
    printLog("[${now().prettyFormat()}] $text")
    if (options.logging) {
        try {
            mqttClient.publish(false, Qos.AT_MOST_ONCE, "$MQTT_LOG/%03d".format(serverId),
                "${now().prettyFormat()} [$serverName] $text".toByteArray().toUByteArray())
        } catch(e: IOException) {}
    }
}
