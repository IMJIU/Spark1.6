package streaming

import java.io.PrintWriter
import java.net.ServerSocket

/**
  * Created by Administrator on 2016/4/28.
  */
object T02_RandomLogger {

  def generateContent(index: Int): String = {
    import scala.collection.mutable.ListBuffer
    var charList = ListBuffer[Char]()
    for (i <- 65 to 90) {
      charList += i.toChar
    }
    val charArray = charList.toArray
    charArray(index).toString
  }

  def index = {
    import java.util.Random
    val rand = new Random
    rand.nextInt(7)
  }

  def main(args: Array[String]) {
    var port = 9999
    var sec = 1000l
    if (args.length >= 2) {
      port = args(0).toInt
      sec = args(1).toLong
    }
    println("listening:" + port)
    val listener = new ServerSocket(port)
    while (true) {
      println("waiting...")
      val socket = listener.accept()
      println("accept:" + socket)
      new Thread() {
        override def run = {
          println("got client:" + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream, true)
          while (true) {
            Thread.sleep(sec)
            val content = generateContent(index)
//            println(content)
                        out.write(content + " " + content + "\n")
//            out.write(content + "\n")
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
