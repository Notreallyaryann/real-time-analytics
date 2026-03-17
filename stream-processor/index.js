
import { Kafka } from 'kafkajs';
import Redis from 'ioredis';
import dotenv from 'dotenv';

dotenv.config();

const redis = new Redis({
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
});

const kafka = new Kafka({
    clientId: 'stream-processor',
    brokers: [process.env.BROKER_URL],
});

const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID });

function getWindowKey(eventType) {
    const now = new Date();
    const year = now.getUTCFullYear();
    const month = String(now.getUTCMonth() + 1).padStart(2, '0');
    const day = String(now.getUTCDate()).padStart(2, '0');
    const hour = String(now.getUTCHours()).padStart(2, '0');
    const minute = String(now.getUTCMinutes()).padStart(2, '0');
    return `stats:${eventType}:${year}${month}${day}:${hour}${minute}`;
}

async function run() {
    await consumer.connect();
    await consumer.subscribe({ topic: process.env.KAFKA_TOPIC, fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const event = JSON.parse(message.value.toString());
                console.log('Processing event:', event.type);

                const windowKey = getWindowKey(event.type);
                await redis.hincrby(windowKey, 'count', 1); //hincrby is atomic

                //In Redis, ZADD is the command used to add members to a Sorted Set
                await redis.zadd('events:timeline', Date.now(), message.value.toString());

            } catch (err) {
                console.error('Error processing message:', err);
            }
        },
    });
}

run().catch(console.error);