import { Kafka } from "kafkajs";
import { v4 as uuid } from "uuid";

const kafka = new Kafka({
  brokers: [process.env.KAFKA_BOOTSTRAP || "localhost:9092"],
});
const p = kafka.producer();
const topic = "events";

function event() {
  const level = Math.random() < 0.7 ? "error" : "warning";
  return {
    source: "sentry",
    service: "api",
    event_type: level === "error" ? "SentryError" : "SentryWarning",
    ts: new Date().toISOString(),
    attributes: { id: uuid(), level },
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
    console.log("[sentry] sent", ev.event_type);
  }, 8_000);
})();
