package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def looper(remainChars: Array[Char], bracketAcc: Int): Boolean = {
      if (bracketAcc < 0) false
      else if (remainChars.isEmpty) bracketAcc == 0
      else remainChars.head match {
        case '(' => looper(remainChars.tail, bracketAcc + 1)
        case ')' => looper(remainChars.tail, bracketAcc - 1)
        case _ => looper(remainChars.tail, bracketAcc)
      }
    }
    looper(chars, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  final case class ExtraBrackets(extraLeft: Int, extraRight: Int) {
    def combine(that: ExtraBrackets): ExtraBrackets = {
      // that should be extra brackets from a sequence of chars at the RHS of the array
      val extras = this.extraLeft - that.extraRight
      if (extras > 0) ExtraBrackets(that.extraLeft + Math.abs(extras), this.extraRight)
      else ExtraBrackets(that.extraLeft, this.extraRight + Math.abs(extras))
    }
  }

  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, extraLeft: Int, extraRight: Int): ExtraBrackets = {
      if (idx == until) ExtraBrackets(extraLeft, extraRight)
      else chars(idx) match {
        case '(' => traverse(idx + 1, until, extraLeft + 1, extraRight)
        case ')' => if (extraLeft > 0) traverse(idx + 1, until, extraLeft - 1, extraRight)
                      else traverse(idx + 1, until, extraLeft, extraRight + 1)
        case _ => traverse(idx + 1, until, extraLeft, extraRight)
      }
    }

    def reduce(from: Int, until: Int): ExtraBrackets = {
      if (until - from < threshold) traverse(from, until, 0, 0) // sequential traverse
      else {
        val mid = (from + until) / 2
        val (left, right) = parallel(reduce(from, mid), reduce(mid, until))
        left combine right
      } // parallel reduce, then combining them in some way
    }

    reduce(0, chars.length) == ExtraBrackets(0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
