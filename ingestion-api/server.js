import express from 'express';
import { Kafka } from 'kafkajs';
import Joi from 'joi';
import { config } from 'dotenv';

config();

const app = express()
app.use(express.json())


//kafka setup
const kafka = new Kafka({
    clientId: 'ingestion-api',
    brokers: [process.env.BROKER_URL]
})
const producer = kafka.producer()

const eventSchema = Joi.object({
    type: Joi.string().required(),
    userId: Joi.string().required(),
    properties: Joi.object().required(),
    timestamp: Joi.date().iso().default(() => new Date().toISOString())
})

app.post('/events', async (req, res) => {
    try {
        // Validate
        const { error, value } = eventSchema.validate(req.body);
        if (error) return res.status(400).json({ error: error.details[0].message });

        //  server timestamp if not provided
        const event = {
            ...value,
            serverTimestamp: new Date().toISOString(),
        };

        // Send to Kafka
        await producer.send({
            topic: process.env.KAFKA_TOPIC,
            messages: [{ value: JSON.stringify(event) }],
        });

        res.status(202).json({ status: 'accepted' });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Internal server error' });
    }
});

async function start() {
    await producer.connect();
    app.listen(process.env.PORT, () => {
        console.log(`Ingestion API listening on port ${process.env.PORT}`);
    });
}

start();
