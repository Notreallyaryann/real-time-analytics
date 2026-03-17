import { Kafka } from 'kafkajs';
import Redis from 'ioredis';
import { InfluxDB, Point } from '@influxdata/influxdb-client';
import dotenv from 'dotenv';

dotenv.config();

//  Initialize Redis
const redis = new Redis({
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
});

//  Initialize InfluxDB
const influxDB = new InfluxDB({
    url: process.env.INFLUXDB_URL,
    token: process.env.INFLUXDB_TOKEN
});
const writeApi = influxDB.getWriteApi(
    process.env.INFLUXDB_ORG,
    process.env.INFLUXDB_BUCKET
);
writeApi.useDefaultTags({ app: 'analytics' });

// Initialize Kafka
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

                //  REDIS LOGIC
                const windowKey = getWindowKey(event.type);
                await redis.hincrby(windowKey, 'count', 1);
                await redis.zadd('events:timeline', Date.now(), message.value.toString());

                // INFLUXDB LOGIC
                const point = new Point('raw_event')
                    .tag('type', event.type)
                    .tag('userId', event.userId || 'anonymous')
                    .intField('value', 1)
                    .timestamp(new Date());

                writeApi.writePoint(point);

                // Flush sends the data to the InfluxDB server
                await writeApi.flush();

            } catch (err) {
                console.error('Error processing message:', err);
            }
        },
    });
}

process.on('SIGINT', async () => {
    await writeApi.close();
    process.exit(0);
});

run().catch(console.error);