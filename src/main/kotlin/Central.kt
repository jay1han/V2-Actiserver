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
        responseText
    } else ""
    printLog("Response=${response.trim()}")
    return response
}

fun selfToCentral() {
    synchronized(Self) {
        Self.lastReport = now()
        mqttLog("v${VERSION_STRING} Alive with " +
                if (Self.actimetreList.isEmpty()) "no Actimetres"
                else Self.actimetreList.keys.sorted().joinToString(separator = " ") {
                    "Actim%04d".format(it) +
                            "@${Self.actimetreList[it]?.frequency}" +
                            "(${Self.actimetreList[it]?.sensorStr()})"
                })
        val reqString = CENTRAL_BIN +
                "action=actiserver&serverId=${serverId}"
        val data = Json.encodeToString(Self.toCentral())
        val registryText = sendHttpRequest(reqString, data)
        if (registryText != "") {
            loadRegistry(registryText)
            printLog("Registry: " + Registry.toString())
        }
    }
}

var mqttClient = MQTTClient(4, MQTT_HOST, MQTT_PORT, null, keepAlive = 0) {}

fun mqttLog(text: String) {
    printLog("[${now().prettyFormat()}] $text")
    if (options.logging) {
        try {
            mqttClient.publish(
                    true, Qos.AT_MOST_ONCE, "$MQTT_LOG/%03d".format(serverId),
                    "${now().prettyFormat()} [$serverName] $text".toByteArray().toUByteArray()
                )
        } catch (e: IOException) {
            try {
                mqttClient = MQTTClient(4, MQTT_HOST, MQTT_PORT, null, keepAlive = 0) {}
            } catch (e: IOException) {}
        }
    }
}
