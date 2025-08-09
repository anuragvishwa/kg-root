import { Kafka } from "kafkajs";
import { v4 as uuid } from "uuid";

const kafka = new Kafka({
  brokers: [process.env.KAFKA_BOOTSTRAP || "localhost:9092"],
});
const p = kafka.producer();
const topic = "events";

function event() {
  const kind = Math.random() < 0.6 ? "LogLine" : "Timeout";
  return {
    source: "loki",
    service: "cache",
    event_type: kind,
    ts: new Date().toISOString(),
    attributes: {
      id: uuid(),
      detail: kind === "Timeout" ? "operation timed out" : "regular log",
    },
  };
}

(async () => {
  await p.connect();
  setInterval(async () => {
    const ev = event();
    await p.send({
      topic,
      messages: [{ key: ev.attributes.id, value: JSON.stringify(ev) }],
    });
    console.log("[loki] sent", ev.event_type);
  }, 5_500);
})();
