const fastify = require("fastify")
const pgp = require('pg-promise')

const database = pgp()("postgres://postgres:postgres@localhost:5432/bank")

const server = fastify()

server.post('/clientes', async (req, res) => {
  const { body } = req

  try {
    const result = await database.query('insert into clientes (nome, limite, saldo) values ($1, $2, $3)', [body.nome, body.limite, body.saldo])
    return res.status(201).send({ result })
  } catch (err) {
    console.log(err.message)
    return res.status(500).send({ message: err.message })
  }
})

server.post('/clientes/:id/transacoes', async (req, res) => {
  const { body, params } = req

  try {
    await database.tx(async t => {
      const [customer] = await t.query('SELECT * FROM clientes WHERE id = $1', [params.id]);
      if (!customer) {
        throw new Error('Cliente não encontrado');
      }
      const balanceUpdated = customer.saldo - body.valor;
      if (balanceUpdated < -customer.limite) {
        throw new Error('Saldo insuficiente');
      }
      await t.query('INSERT INTO transacoes (cliente_id, valor, tipo, descricao, realizada_em) VALUES ($1, $2, $3, $4, $5)', [params.id, body.valor, body.tipo, body.descricao, new Date().toISOString()]);

      const updateResult = await t.query('UPDATE clientes SET saldo = $1, versao = versao + 1 WHERE id = $2 AND versao = $3', [balanceUpdated, params.id, customer.versao]);
      if (updateResult.rowCount === 0) {
        throw new Error('Os dados do cliente foram alterados, tente novamente.');
      }
    });
    return res.status(201).send(customer)
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
  try {
    const [customer] = await database.query('select * from clientes where id = $1', [params.id])
    console.log("🚀 ~ server.get ~ customer:", customer)
    if (!customer) {
      return res.status(404).send()
    }
    const transactions = await database.query('select valor, tipo, descricao, realizada_em from transacoes where cliente_id = $1', [params.id])
    return res.status(200).send({
      "saldo": {
        "total": customer.saldo,
        "data_extrato": new Date().toISOString(),
        "limite": customer.limite
      },
      "ultimas_transacoes": transactions
    })
  } catch (err) {
    console.log(err.message)
    return res.status(500).send({ message: err.message })
  }
})

server.listen({ port: 3333 }, (err, address) => {
  if (err) throw err
  console.log("Server is running")
})