import { Kafka } from "kafkajs";
import { v4 as uuid } from "uuid";

const kafka = new Kafka({
  brokers: [process.env.KAFKA_BOOTSTRAP || "localhost:9092"],
});
const p = kafka.producer();
const topic = "events";

function event() {
  const state = Math.random() < 0.5 ? "CrashLoopBackOff" : "PodStarted";
  return {
    source: "k8s",
    service: "worker",
    event_type: state,
    ts: new Date().toISOString(),
    attributes: {
      id: uuid(),
      pod: `pod-${Math.floor(Math.random() * 10)}`,
      state,
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
    console.log("[k8s] sent", ev.event_type);
  }, 7_000);
})();
