const { Kafka, logLevel } = require('kafkajs')

const host = 'localhost'

const kafka = new Kafka({
    logLevel: logLevel.INFO,
    brokers: [`${host}:9092`],
    clientId: 'example-consumer',
})
const admin = kafka.admin()

const run = async () => {
    await admin.connect()

    const orders = 'orders'
    const numPartitions = 3

    const ordersTopic = { topic: orders, numPartitions: numPartitions }
    const ordersPartition = { topic: orders, count: numPartitions }

    try {
        await admin.createTopics({ topics: [ordersTopic] })
        await admin.createPartitions({ topicPartitions: [ordersPartition] })
    } catch (error) {
        console.error(`Error creating topics! Error: ${error}`)
    }

    await admin.disconnect()
}

run().catch(e => console.error(`Error on kafka admin ${e.message}`, e))
