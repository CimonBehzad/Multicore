import scala.util._
import java.util.Scanner
import java.io._
import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.io._

case class Flow(f: Int)
case class Push(f: Int)
case class Debug(debug: Boolean)
case class Control(control: ActorRef)
case class Source(n: Int)
case class GetHeight(ret: ActorRef)
case class IncreaseActive()
case class DecreaseActive()

/** Tries to push amount to neighbor Neighbor check if this.height < myHeight,
  * if so updates excess and edges else pushes back with Push
  *
  * @param amount
  * @param myHeight
  */
case class TryPush(amount: Int, myHeight: Int, edge: Edge)

/** Unconditionally pushes amount to neighbor
  *
  * @param amount
  */
case class Ack(amount: Int)

case object Print
case object Start
case object Excess
case object Maxflow
case object Sink
case object Hello

class Edge(var source: ActorRef, var target: ActorRef, var c: Int) {
    var f = 0
}

class Node(val index: Int) extends Actor {
    var e = 0; /* excess preflow. 						*/
    var h = 0; /* height. 							*/
    var control: ActorRef =
        null /* controller to report to when e is zero. 			*/
    var source: Boolean = false /* true if we are the source.					*/
    var sink: Boolean = false /* true if we are the sink.					*/
    var edges: List[Edge] =
        Nil /* adjacency list with edges objects shared with other nodes.	*/
    var debug = true /* to enable printing.	*/
    var nextEdge = 0;
    var semaphoreCount = 0;

    def min(a: Int, b: Int): Int = { if (a < b) a else b }

    def id: String = "@" + index;

    def other(a: Edge, u: ActorRef): ActorRef = {
        if (u == a.source) a.target else a.source
    }

    def status: Unit = {
        if (debug)
            println(
              id + " e = " + e + ", h = " + h + " Semaphore count: " + semaphoreCount + " Nextedge: " + nextEdge
            )
    }
    def debugPrint(s: String): Unit = { if (debug) println(id + ": " + s) }

    def enter(func: String): Unit = {
        if (debug) { println(id + " enters " + func); status }
    }
    def exit(func: String): Unit = {
        if (debug) { println(id + " exits " + func); status }
    }

    def relabel: Unit = {

        enter("relabel")

        h += 1

        exit("relabel")
    }

    def work: Unit = {
        enter("work" + " " + "nextedge: " + nextEdge)
        if (sink || source) {
            exit("work sink/source")
            return;
        }
        if (semaphoreCount != 0) {
            return;
        }
        var remaining = e
        while (remaining > 0) {
            if (nextEdge >= edges.length) { // We have tried to push to everyone but no one worked, we need to raise
                nextEdge = 0;
                relabel
            }
            if (edges.length == 0) {
                println("Node " + id + " has no edges!")
            }
            val edge = edges(nextEdge);
            nextEdge += 1
            var pushCapacity = 0;
            if (edge.source == self) {
                pushCapacity = edge.c - edge.f
            } else {
                pushCapacity = edge.f + edge.c // We push back
            }
            if (pushCapacity == 0) {
                debugPrint(
                  "No push capacity on edge from " + index + " on edge index " + (nextEdge - 1) + " Capacity: " + edge.c + " Flow: " + edge.f
                )
                work // We can not push on this edge, go to next
                return;
            }
            val delta = min(remaining, pushCapacity)

            semaphoreCount += 1
            other(edge, self) ! TryPush(delta, h, edge)
            remaining -= delta
            debugPrint(
              "Pushed " + delta + " from " + index + " to " + other(
                edge,
                self
              ) + " Edge Height: " + h + " Capacity: " + edge.c + " Flow: " + edge.f
            )

        }
        exit("work")

    }

    def receive = {

        case Debug(debug: Boolean) => this.debug = debug

        case Print => status

        case Excess => {
            enter("Excess")
            sender ! Flow(e)
            exit("Excess")
            /* send our current excess preflow to actor that asked for it. */
        }

        case edges: Edge => {
            this.edges =
                edges :: this.edges /* put this edges first in the adjacency-list. */
        }

        case Control(control: ActorRef) => this.control = control

        case Sink => { sink = true }

        case Ack(c) => {
            enter("Ack " + c)
            val wasActive = e > 0
            e -= c
            semaphoreCount -= 1
            if (
              e == 0 && wasActive && semaphoreCount == 0 && !source && !sink
            ) {
                enter("Notify control of DecreaseActive")
                control ! DecreaseActive
            } else if (semaphoreCount == 0) {
                work
            }
            exit("Ack " + c)
        }

        case TryPush(amount: Int, senderHeight: Int, edge: Edge) => {
            enter("Gets TryPushed" + " " + amount + " " + senderHeight)

            if (h >= senderHeight) { // We higher than who sent to us, we stop sender
                enter("Rejected" + " " + amount)
                other(edge, self) ! Ack(0) // NACK
            } else {
                if (e == 0 && amount != 0 && !source && !sink) {
                    control ! IncreaseActive
                }
                e += amount
                if (edge.target == self)
                    edge.f += amount
                else {
                    edge.f -= amount
                }
                work
                other(edge, self) ! Ack(amount) // We accept amount
            }

            exit("Done Getting TryPushed" + " " + amount)
        }

        case Source(n: Int) => {
            println("Source started")
            h = n;
            source = true
            for (a <- edges) {

                val v = other(a, self)
                a.f = a.c
                println(
                  "Sending initial push of " + a.c + " from " + index + " to " + v
                )
                v ! TryPush(a.c, h, a)
            }
        }

        case _ =>
            {
                println("" + index + " received an unknown message" + _)
            }

            assert(false)
    }

}

class Preflow extends Actor {
    var s = 0; /* index of source node.					*/
    var t = 0; /* index of sink node.					*/
    var n = 0; /* number of vertices in the graph.				*/
    var edges: Array[Edge] = null /* edges in the graph.						*/
    var node: Array[ActorRef] = null /* vertices in the graph.					*/
    var ret: ActorRef = null /* Actor to send result to.

     */
    var active = 0;

    def receive = {

        case node: Array[ActorRef] => {
            this.node = node
            n = node.size
            s = 0
            t = n - 1
            for (u <- node)
                u ! Control(self)
        }

        case edges: Array[Edge] => this.edges = edges

        case Flow(f: Int) => {
            println("Got flow " + f);

            ret ! f /* somebody (hopefully the sink) told us its current excess preflow. */

        }

        case Maxflow => {
            println("Got maxflow")
            ret = sender

        }

        case IncreaseActive => {
            active += 1
            println("Increasing: Active nodes: " + active)

        }

        case DecreaseActive => {
            active -= 1
            println("Decreasing: Active nodes: " + active)
            if (active == 0) {
                node(
                  t
                ) ! Excess /* ask sink for its excess preflow (which certainly still is zero). */
            }
        }
    }
}

object main extends App {
    implicit val t = Timeout(4 seconds);

    val begin = System.currentTimeMillis()
    val system = ActorSystem("Main")
    val control = system.actorOf(Props[Preflow], name = "control")

    var n = 0;
    var m = 0;
    var edges: Array[Edge] = null
    var node: Array[ActorRef] = null

    val s = new Scanner(System.in);

    n = s.nextInt
    m = s.nextInt

    /* next ignore c and p from 6railwayplanning */
    s.nextInt
    s.nextInt

    node = new Array[ActorRef](n)

    for (i <- 0 to n - 1)
        node(i) = system.actorOf(Props(new Node(i)), name = "v" + i)

    edges = new Array[Edge](m)

    for (i <- 0 to m - 1) {

        val u = s.nextInt
        val v = s.nextInt
        val c = s.nextInt

        edges(i) = new Edge(node(u), node(v), c)

        node(u) ! edges(i)
        node(v) ! edges(i)
    }

    node(0) ! Source(n)
    node.last ! Sink

    control ! node
    control ! edges

    val flow = control ? Maxflow
    val f = Await.result(flow, t.duration)

    println("f = " + f)

    system.stop(control);
    system.terminate()

    val end = System.currentTimeMillis()

    println("t = " + (end - begin) / 1000.0 + " s")
    println("Final state:")
    for (edge <- edges) {
        println(
          "Edge from " + edge.source.path.name + " to " + edge.target.path.name + " with capacity " + edge.c + " has flow " + edge.f
        )
    }

}
