import { createServer } from 'http'
import staticHandler from 'serve-handler'
import ws, { WebSocketServer } from 'ws'
import yargs from 'yargs'
import { hideBin } from 'yargs/helpers'
import zmq from 'zeromq'

const server = createServer((req, res) => {
  return staticHandler(req, res, { public: 'www' })
})

console.log(yargs(hideBin(process.argv)).argv)

let pubSocket
async function initializeSockets () {
  pubSocket = new zmq.Publisher()
  await pubSocket.bind(`tcp://127.0.0.1:${yargs(hideBin(process.argv)).argv.pub}`)

  const subSocket = new zmq.Subscriber()
  const subPorts = [].concat(yargs(hideBin(process.argv)).argv.sub)
  for (const port of subPorts) {
    console.log(`Subscribing to ${port}`)
    subSocket.connect(`tcp://127.0.0.1:${port}`)
  }
  subSocket.subscribe('chat')

  for await (const [msg] of subSocket) {
    console.log(`Message from another server: ${msg}`)
    broadcast(msg.toString().split(' ').slice(1).join(' '))
  }
}

initializeSockets()

const wss = new WebSocketServer({ server })
wss.on('connection', client => {
  console.log('Client connected')
  client.on('message', msg => {
    console.log(`Message: ${msg}`)
    broadcast(msg)
    pubSocket.send(`chat ${msg}`)
  })
})

function broadcast (msg) {
  for (const client of wss.clients) {
    if (client.readyState === ws.OPEN) {
      client.send(msg)
    }
  }
}

server.listen(yargs(hideBin(process.argv)).argv.http || 8080)
