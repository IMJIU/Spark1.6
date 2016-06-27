package streaming

import java.io._
import java.net.Socket

/**
  * Created by Administrator on 2016/4/28.
  */
object T02_Test {
  def main(args: Array[String]) {
    val socket = new Socket("localhost", 9999)
    val in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
    val out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true)
    println("reading!....")
    var line = in.readLine()
    while (line != null) {
      println(line)
      line = in.readLine()
    }
  }
}
