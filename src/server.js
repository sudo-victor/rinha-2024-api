const fastify = require("fastify")
const Redis = require('ioredis');
const pgp = require('pg-promise')();
const amqp = require('amqplib');

async function connectRabbitMQ() {
  const connection = await amqp.connect('amqp://user:queue_pass@localhost:5672');
  const channel = await connection.createChannel();
  await channel.assertQueue('transacoesQueue');
  return { connection, channel };
}

const dbConfig = {
  host: 'localhost',
  port: 5432,
  database: 'bank',
  user: 'postgres',
  password: 'postgres',
  max: 10,
  idleTimeoutMillis: 30000,
};
const database = pgp(dbConfig);
const redis = new Redis({ host: 'localhost', port: 6379 });
const server = fastify()

server.post('/clientes', async (req, res) => {
  const { body } = req
  try {
    const result = await database.query('insert into clientes (nome, limite, saldo) values ($1, $2, $3)', [body.nome, body.limite, body.saldo])
    return res.status(201).send({ result })
  } catch (err) {
    return res.status(500).send({ message: err.message })
  }
})

server.post('/clientes/:id/transacoes', async (req, res) => {
  const { body, params } = req
  const { channel: queueChannel } = await connectRabbitMQ();

  try {
    let customer = null
    const customerCached = await redis.get(`customer:${params.id}`)
    if (customerCached) {
      customer = JSON.parse(customerCached)
    } else {
      const [customerDatabase] = await database.query('SELECT * FROM clientes WHERE id = $1', [params.id]);
      customer = customerDatabase
    }
    if (!customer) {
      throw new Error('Cliente não encontrado');
    }
    const balanceUpdated = customer.saldo - body.valor;
    if (balanceUpdated < -customer.limite) {
      throw new Error('Saldo insuficiente');
    }

    queueChannel.sendToQueue('transacoesQueue', Buffer.from(JSON.stringify({ customerId: params.id, ...body })))

    return res.status(200).send({
      saldo: balanceUpdated,
      limite: customer.limite
    })
  } catch (err) {
    if (err.message === 'Cliente não encontrado') {
      return res.status(404).send({ message: err.message })
    }
    if (err.message === 'Saldo insuficiente') {
      return res.status(422).send({ message: err.message })
    }
    if (err.message === 'Os dados do cliente foram alterados, tente novamente.') {
      return res.status(422).send({ message: err.message })
    }
    return res.status(500).send({ message: err.message })
  }
})

server.get('/clientes/:id/extrato', async (req, res) => {
  const { params } = req
  const cacheKey = `extract:${params.id}`
  try {
    const cachedData = await redis.get(cacheKey);
    if (cachedData) {
      return res.status(200).send(JSON.parse(cachedData))
    }
    const [customer] = await database.query('select * from clientes where id = $1', [params.id])
    if (!customer) {
      return res.status(404).send()
    }
    const transactions = await database.query('select valor, tipo, descricao, realizada_em from transacoes where cliente_id = $1', [params.id])
    const result = {
      "saldo": {
        "total": customer.saldo,
        "data_extrato": new Date().toISOString(),
        "limite": customer.limite
      },
      "ultimas_transacoes": transactions
    }
    await redis.set(cacheKey, JSON.stringify(result), 'EX', 3600);
    return res.status(200).send(result)
  } catch (err) {
    return res.status(500).send({ message: err.message })
  }
})

server.listen({ port: 3333 }, (err, address) => {
  if (err) throw err
  console.log("Server is running")
})

async function transactionQueueConsumer() {
  const { channel } = await connectRabbitMQ();
  let customerUpdated = null
  channel.consume('transacoesQueue', async message => {
    try {
      const transactionDetails = JSON.parse(message.content.toString());
      await database.tx(async t => {
        const [customer] = await t.query('SELECT * FROM clientes WHERE id = $1', [transactionDetails.customerId]);
        if (!customer) {
          throw new Error('Cliente não encontrado');
        }
        const balanceUpdated = customer.saldo - transactionDetails.valor;
        if (balanceUpdated < -customer.limite) {
          throw new Error('Saldo insuficiente');
        }
        await t.query('INSERT INTO transacoes (cliente_id, valor, tipo, descricao, realizada_em) VALUES ($1, $2, $3, $4, $5)', [transactionDetails.customerId, transactionDetails.valor, transactionDetails.tipo, transactionDetails.descricao, new Date().toISOString()]);

        const updateResult = await t.query('UPDATE clientes SET saldo = $1, versao = versao + 1 WHERE id = $2 AND versao = $3', [balanceUpdated, transactionDetails.customerId, customer.versao]);
        if (updateResult.rowCount === 0) {
          throw new Error('Os dados do cliente foram alterados, tente novamente.');
        }
        customerUpdated = { ...customer, saldo: balanceUpdated, versao: customer.versao + 1 }
      });

      await redis.del(`extract:${transactionDetails.customerId}`)
      if (customerUpdated) {
        await redis.set(`customer:${transactionDetails.customerId}`, JSON.stringify(customerUpdated))
      }
      channel.ack(message);
    } catch (err) {
      console.log(err.message)
    }
  });
}

transactionQueueConsumer()