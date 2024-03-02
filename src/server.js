const dotenv = require('dotenv');
dotenv.config();
const fastify = require("fastify");
const pgp = require('pg-promise')();
const { connect } = require('nats');

class Queue {
  static instance = null

  static async getInstance() {
    if (!Queue.instance) {
      this.instance = await connect({ servers: process.env.QUEUE_URL })
      return this.instance
    }

    return this.instance
  }
}

const dbConfig = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  max: 10,
  idleTimeoutMillis: 30000,
};

Queue.getInstance()

const database = pgp(dbConfig);
const server = fastify();

server.post('/clientes', async (req, res) => {
  const { body } = req
  try {
    const result = await database.query('insert into clientes (nome, limite, saldo) values ($1, $2, $3)', [body.nome, body.limite, body.saldo])
    return res.status(201).send({ result })
  } catch (err) {
    return res.status(500).send({ message: err.message })
  }
});

server.post('/clientes/:id/transacoes', async (req, res) => {
  const { body, params } = req
  if (![1,2,3,4,5].includes(parseInt(params.id))) throw new Error('Cliente não encontrado');
  try {
    const [customer] = await database.query('SELECT * FROM clientes WHERE id = $1', [params.id]);
    if (!customer) throw new Error('Cliente não encontrado');
    // validate balance
    const balanceUpdated = body.tipo === "d" ? customer.saldo - body.valor : customer.saldo + body.valor;
    if (balanceUpdated < -customer.limite) throw new Error('Saldo insuficiente');
    const queue = await Queue.getInstance()

    queue.publish('transacoesQueue', JSON.stringify({ customerId: params.id, ...body }));

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
});

server.get('/clientes/:id/extrato', async (req, res) => {
  const { params } = req
  try {
    const [customer] = await database.query('select * from clientes where id = $1', [params.id])
    if (!customer) {
      return res.status(404).send()
    }
    const transactions = await database.query('select valor, tipo, descricao, realizada_em from transacoes where cliente_id = $1 ORDER BY realizada_em DESC LIMIT 10', [params.id])
    return res.status(200).send({
      "saldo": {
        "total": customer.saldo,
        "data_extrato": new Date().toISOString(),
        "limite": customer.limite
      },
      "ultimas_transacoes": transactions
    })
  } catch (err) {
    return res.status(500).send({ message: err.message })
  }
});

server.listen({
  host: '0.0.0.0',
  port: process.env.PORT
}, (err, address) => {
  if (err) throw err;
  console.log(`Server is running at ${address}`);
});
