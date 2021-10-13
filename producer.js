const { Kafka, CompressionTypes, logLevel } = require('kafkajs')

const host = 'localhost'

const kafka = new Kafka({
  logLevel: logLevel.DEBUG,
  brokers: [`${host}:9092`],
  clientId: 'example-producer',
})

const topic = 'orders'
const producer = kafka.producer()

const getRandomAmount = () => Math.round(Math.random(10) * 5)
const getRandomKey = () => Math.round(Math.random(10) * 100)

const createMessage = (key, vale) => ({
  key: key,
  value: vale,
})

const createMessages = (amount) => {
  const messages = []
  const key = `KEY '${getRandomKey()}''`

  for (let i = 0; i < amount; i++) {
    const value = `msg index '${i}' in group with '${amount}' messages`

    const message = createMessage(key, value)
    messages.push(message)
  }

  return messages
}

const sendMessage = async () => {
  try {
    const messagesAmount = getRandomAmount()
    const messages = createMessages(messagesAmount)

    await producer.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: messages,
    })

    return
  } catch (e) {
    return console.error(`[example/producer] ${e.message}`, e)
  }
}

const run = async () => {
  const interval = 10
  // const interval = 1000

  await producer.connect()
  setInterval(
    async () => { await sendMessage() },
    interval
  )
}

run().catch(e => console.error(`[example/producer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async () => {
    try {
      console.log(`process.on ${type}`)
      await producer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      await producer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})