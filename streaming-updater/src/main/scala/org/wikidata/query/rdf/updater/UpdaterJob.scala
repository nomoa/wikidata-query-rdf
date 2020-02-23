package org.wikidata.query.rdf.updater

import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ListBuffer

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory
import org.wikidata.query.rdf.tool.change.events.RevisionCreateEvent
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris

/**
 * Current state
 * stream1 = kafka(with periodic watermarks)
 *  => filter(domain == "something")
 *  => map(event convertion)
 * stream2 = same principle
 *
 * union of all streams
 *  => keyBy (used as a partitioner to reduce cardinality)
 *  => timeWindow(1min)
 *  => late events goes to LATE_EVENTS_SIDE_OUPUT_TAG
 *  => process(reorder the events within the window) see EventReordering
 *  => keyBy item
 *  => map(decide mutation ope) see DecideMutationOperation
 *  => process(remove spurious events) see RouteIgnoredMutationToSideOutput
 *
 *  output of the stream a MutationOperation
 *  TODO: fetch info from Wikibase, generate the diff, output triples
 */
object UpdaterJob {
  val LATE_EVENTS_SIDE_OUPUT_TAG = new OutputTag[InputEvent]("late-events")
  val SPURIOUS_REV_EVENTS = new OutputTag[IgnoredMutation]("spurious-rev-events")
  val REORDERING_WINDOW_LENGTH = 60000L

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val sourceHostname = params.get("hostname")
    val kafkaBrokers = params.get("brokers");
    val checkpointDir = params.get("checkpoint_dir")
    val spuriousEventsDir = params.get("spurious_events_dir")
    val lateEventsDir = params.get("late_events_dir")
    val outputDir = params.get("output_dir")
    val revCreateTopic = params.get("rev_create_topic")
    implicit val uris: Uris = WikibaseRepository.Uris.fromString(s"https://$sourceHostname")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(new RocksDBStateBackend(checkpointDir))
    env.enableCheckpointing(30000)
    val props = new Properties()
    props.setProperty("bootstrap.servers", kafkaBrokers)
    props.setProperty("group.id", "test_wdqs_streaming_updater")
    val maxLateness: Long = 60000
    val revCreateEventStream: DataStream[RevisionCreateEvent] = env
      .addSource(new FlinkKafkaConsumer(revCreateTopic, new RevisionCreateEventJson(), props))
      // Use periodic watermark with kafka so that
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[RevisionCreateEvent](Time.milliseconds(maxLateness)) {
        override def extractTimestamp (element: RevisionCreateEvent): Long = element.timestamp().toEpochMilli
      })
      .uid(s"$revCreateTopic[${classOf[RevisionCreateEvent].getSimpleName}]")
      .name(s"$revCreateTopic[${classOf[RevisionCreateEvent].getSimpleName}]")
    val (mainStream, lateEvents) = prepare(prepareRevisionCreateStream(revCreateEventStream) :: Nil)

    mainStream.addSink(prepareJsonDebugSink(outputDir): StreamingFileSink[MutationOperation])
      .name("output")
      .uid("output")
    lateEvents.addSink(prepareJsonDebugSink(lateEventsDir): StreamingFileSink[InputEvent])
      .name("late events")
      .uid("late_events")
    mainStream.getSideOutput(SPURIOUS_REV_EVENTS).addSink(prepareJsonDebugSink(spuriousEventsDir): StreamingFileSink[IgnoredMutation])
      .name("spurious events")
      .uid("spurious_events")
    env.execute("WDQS Stream Updater (quick&dirty POC)")
  }


  def prepareJsonDebugSink[O](outputPath: String): StreamingFileSink[O] = {
    StreamingFileSink.forRowFormat(new Path(outputPath),
      new Encoder[O] {
        override def encode(element: O, stream: OutputStream): Unit = {
          stream.write(s"$element\n".getBytes(StandardCharsets.UTF_8))
        }
      })
      .withRollingPolicy(DefaultRollingPolicy.builder()
        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
        .withMaxPartSize(1024 * 1024 * 1024)
        .build())
      .build()
  }

  def prepareRevisionCreateStream(rawStream: DataStream[RevisionCreateEvent])(implicit uris: Uris): DataStream[InputEvent] = {
      rawStream
        .filter(new RevisionCreateEventFilter(uris.getHost))
      .map(e => Rev(e.title(), e.timestamp(), e.revision()): InputEvent)
  }

  def prepare(streams: List[DataStream[InputEvent]] = Nil,
              reorderWindow: Long = REORDERING_WINDOW_LENGTH): (DataStream[MutationOperation], DataStream[InputEvent]) = {
    val inputStream = streams match {
      case Nil => throw new NoSuchElementException("at least one stream is needed")
      case x :: Nil => x
      case x :: rest => x.union(rest: _*)
    }
    def partitioner(inputEvent: InputEvent): Int = {
      // fake partitioning, assumption here is that going with keyBy(item) might lead to unnecessarily high cardinality
      // so here we ponder cardinality vs number of elements to sort in a window
      // Really not sure if this is a valid concern to have
      // - naive: Going high card where most of the windows will have a single element
      // - this idea: Going with large windows state we have a costly sort operation
      ".(\\d+)".r.findAllIn(inputEvent.item).matchData.toList.head.group(1).toInt % 10
    }
    val windowStream = inputStream
        .keyBy(inputStream => partitioner(inputStream))
        .timeWindow(Time.milliseconds(reorderWindow))
        // NOTE: allowing lateness here is tricky to handle as it might fire the same window multiple times
        // https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html#late-elements-considerations
        // hoping that this is mitigated by using BoundedOutOfOrdernessTimestampExtractor which emits late watermark so that
        // the window is triggered only after its max lateness is reached
        //.allowedLateness(Time.milliseconds(maxLateness))
        .sideOutputLateData(LATE_EVENTS_SIDE_OUPUT_TAG)
        .process(new EventReordering())
        .uid("EventReordering")
      val sideOutput = windowStream.getSideOutput(LATE_EVENTS_SIDE_OUPUT_TAG)
      // FIXME: ugly but somehow the side output is lost after another keyBy (or map?)
      val mainStream = windowStream
        .keyBy(_.item)
        .map(new DecideMutationOperation())
        .uid("DecideMutationOperation")
        .process(new RouteIgnoredMutationToSideOutput())
      (mainStream, sideOutput)
  }

  sealed class RevisionCreateEventFilter(hostname: String) extends FilterFunction[RevisionCreateEvent] {
    lazy val urisScheme = UrisSchemeFactory.forHost(hostname)
    lazy val uris = Uris.fromString(s"https://$hostname")
    override def filter(e: RevisionCreateEvent): Boolean = {
      e.domain() == uris.getHost && urisScheme.supportsInitial(e.title())
    }
  }
  sealed class DecideMutationOperation extends RichMapFunction[InputEvent, MutationOperation] {
    // FIXME: use java.lang.Long for nullability
    //  (flink's way for detecting that the state is not set for this entity)
    private var state: ValueState[java.lang.Long] = _
    override def map(ev: InputEvent): MutationOperation = {
      ev match {
        case rev: Rev => {
          val lastRev = state.value()
          if (lastRev == null) {
            state.update(rev.revision)
            FullImport(rev.item, rev.eventTime, rev.revision)
          } else if (lastRev <= rev.revision) {
            state.update(rev.revision)
            Diff(rev.item, rev.eventTime, rev.revision, lastRev)
          } else {
            // Event related to an old revision
            // It's too late to handle it
            IgnoredMutation(rev.item, rev.eventTime, rev.revision, rev)
          }
        }
      }
    }
    override def open(parameters: Configuration): Unit = {
      state = getRuntimeContext.getState(new ValueStateDescriptor[java.lang.Long]("lastSeenRev", createTypeInformation[java.lang.Long]))
    }
  }

  sealed class RouteIgnoredMutationToSideOutput(ignoredEventTag: OutputTag[IgnoredMutation] = SPURIOUS_REV_EVENTS)
    extends ProcessFunction[MutationOperation, MutationOperation]
  {
    override def processElement(i: MutationOperation,
                                context: ProcessFunction[MutationOperation, MutationOperation]#Context,
                                collector: Collector[MutationOperation]
                               ): Unit = {
      i match {
        case e: IgnoredMutation => context.output(ignoredEventTag, e)
        case x: Any => collector.collect(x)
      }
    }
  }

  sealed class EventReordering extends ProcessWindowFunction[InputEvent, InputEvent, Int, TimeWindow] {
    private var buffer: ListState[InputEvent] = _

    override def open(parameters: Configuration): Unit = {
      val stateDesc = new ListStateDescriptor[InputEvent]("re-ordering-state",
        TypeInformation.of(classOf[InputEvent]))
      this.buffer = getRuntimeContext.getListState(stateDesc)
    }

    override def process(key: Int, context: Context, inputEvents: Iterable[InputEvent], out: Collector[InputEvent]): Unit = {
      val toReorder = new ListBuffer[InputEvent]
      for (event <- inputEvents) {
        toReorder.append(event)
      }
      toReorder
        .sortBy(e => (e.item, e.revision, e.eventTime))
        .foreach(out.collect)
    }
  }

  sealed trait InputEvent {
    val item: String
    val eventTime: Instant
    val revision: Long
  }
  final case class Rev(item: String, eventTime: Instant, revision: Long) extends InputEvent

  sealed trait MutationOperation {
    val item: String
    val eventTime: Instant
    val revision: Long
  }
  final case class Diff(item: String, eventTime: Instant, revision: Long, fromRev: Long) extends MutationOperation
  final case class FullImport(item: String, eventTime: Instant, revision: Long) extends MutationOperation
  final case class IgnoredMutation(item: String, eventTime: Instant, revision: Long, inputEvent: InputEvent) extends MutationOperation
}
