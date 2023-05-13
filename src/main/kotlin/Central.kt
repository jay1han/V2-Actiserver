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
    val centralURL = URL("HTTP", CENTRAL_HOST, reqString)
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
    Self.lastReport = now()
    mqttLog("Reporting to Central")
    val reqString = CENTRAL_BIN +
            "action=actiserver&serverId=${serverId}&ip=${myIp}&mac=${myMac}"
    val data = Json.encodeToString(Self)
    val registryText = sendHttpRequest(reqString, data)
    loadRegistry(registryText)
    printLog("Registry: " + Registry.toString())
}

lateinit var mqttClient: MQTTClient

fun mqttLog(text: String) {
    printLog(text)
    if (options.logging) {
        try {
            mqttClient.publish(false, Qos.AT_MOST_ONCE, MQTT_LOG,
                "$serverName: $text".toByteArray().toUByteArray())
        } catch(e: IOException) {}
    }
}
