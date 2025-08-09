import { Kafka } from "kafkajs";
import { v4 as uuid } from "uuid";

const kafka = new Kafka({
  brokers: [process.env.KAFKA_BOOTSTRAP || "localhost:9092"],
});
const p = kafka.producer();
const topic = "events";

function event() {
  const metric = Math.random() < 0.5 ? "CPU" : "Memory";
  return {
    source: "prom",
    service: "cart",
    event_type: metric === "CPU" ? "CPUOverload" : "MemoryPressure",
    ts: new Date().toISOString(),
    attributes: { id: uuid(), metric, value: Math.random() * 100 },
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
    console.log("[prom] sent", ev.event_type);
  }, 5_000);
})();
