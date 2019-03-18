import java.util

import com.codahale.metrics.MetricRegistry
import org.coursera.metrics.datadog.DatadogReporter
import org.coursera.metrics.datadog.DatadogReporter.Expansion._
import org.coursera.metrics.datadog.transport.Transport
import org.coursera.metrics.datadog.transport.HttpTransport
import org.coursera.metrics.datadog.transport.UdpTransport

import scala.concurrent.duration.SECONDS

object DataDogTest extends App {

  val metrics = new MetricRegistry();
  val requests = metrics.meter("test");
  val expansions = util.EnumSet.of(COUNT, RATE_1_MINUTE, RATE_15_MINUTE, MEDIAN, P95, P99)
  val httpTransport = new HttpTransport.Builder().withApiKey("257990b896642192db8fd0a8d31e1993").withProxy("",1).build()
  val reporter = DatadogReporter.forRegistry(metrics)
    .withTransport(httpTransport)
    .withExpansions(expansions)
    .build()

  reporter.start(10, SECONDS)

  while (true){
    requests.mark(10)
    println("send mark")
    Thread.sleep(1000)
  }

}
