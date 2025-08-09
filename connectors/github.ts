import { Kafka } from "kafkajs";
import { v4 as uuid } from "uuid";

const kafka = new Kafka({
  brokers: [process.env.KAFKA_BOOTSTRAP || "localhost:9092"],
});
const p = kafka.producer();
const topic = "events";

function event() {
  const action = Math.random() < 0.5 ? "GitPush" : "GitPR";
  return {
    source: "github",
    service: "webapp",
    event_type: action,
    ts: new Date().toISOString(),
    attributes: { id: uuid(), repo: "frontend", action },
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
    console.log("[github] sent", ev.event_type);
  }, 6_000);
})();
