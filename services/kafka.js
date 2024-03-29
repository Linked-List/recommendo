const kafka = require("kafka-node");

const PRODUCER_OPTIONS = {

};
const CUSTOMER_OPTIONS = {

};
const CONSUMER_TOPIC = "tfIdfResults";
const KAFKA_HOST = "localhost:9092"
let IS_PRODUCER_READY = 0;

const keywordResMap = new Map();

const client = new kafka.KafkaClient({ kafkaHost: KAFKA_HOST });
const stuckedMessages = [];

// init producer
const producer = new kafka.Producer(client);
producer.on("ready", async () => {
    IS_PRODUCER_READY = 1;
});
const consumer = new kafka.Consumer(client, [
    { topic: CONSUMER_TOPIC, partition: 0 }
]);
consumer.on("message", (message) => {

    const word = message.key;
    const result = message.value;
    const resArr = keywordResMap.get(word);
    const nowms = Date.now();
    if (resArr) {
        for (const res of resArr) {
            res.status(201).json({
                "resultCode": 201,
                "resultMsg": "Successfully calc related keywords",
                "item": {
                    "keyword": word,
                    "relatedKeywords": result,
                    "lastModified": nowms,
                }
            });
        }
        keywordResMap.delete(word);
    } else {
        throw new CustomError("Cannot find res object", 500);
    }
});

class KafkaDriver {
    static sendMessage(topic, message) {
        if (IS_PRODUCER_READY) {
            const keyedMessage = new kafka.KeyedMessage(message.key, message.value)
            const kafkaMessage = [{ topic, messages: keyedMessage }];
            producer.send(kafkaMessage, (error, data) => {
                if (error) {
                    throw new CustomError("Kafka producer send message error", 500, error);
                } else {
                    console.log("send message!");
                    console.log(data);
                }
            });
        } else {
            console.log("producer is not ready");
            stuckedMessages.push({ topic, messages });
        }
    }

    static async createTopic(topic) {
        // To check topic already exists
        // If exist, do not create topic
        try {
            await new Promise((resolve, reject) => {
                client.loadMetadataForTopics([topic], (error, result) => {
                    const topics = Object.keys(result[1].metadata);
                    if (topics.includes(topic))
                        reject(result);
                    else
                        resolve();
                });
            });
        } catch (error) {
            return;
        }

        const topicsToCreate = [
            {
                topic,
                partitions: 1,
                replicationFactor: 1,
            },
        ];
        return new Promise((resolve, reject) => {
            client.createTopics(topicsToCreate, (error, result) => {
                if (result) {
                    reject(result);
                } else {
                    console.log(`[Kafka] Create topic '${topic}' Successfully`);
                    resolve();
                }
            });
        });
    }
}

module.exports = {
    KafkaDriver,
    keywordResMap,
};
