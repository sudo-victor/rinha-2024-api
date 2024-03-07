const dotenv = require('dotenv');
dotenv.config();
const fastify = require("fastify");
const pgp = require('pg-promise')();
const yup = require("yup")

const dbConfig = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  max: 10,
  idleTimeoutMillis: 30000,
};

const database = pgp(dbConfig);
const server = fastify();

server.post('/clientes', async (req, res) => {
    const { body } = req;
    if (!body.nome || !body.limite || body.saldo == null) {
        return res.status(400).send({ message: 'Invalid request data' });
    }
    try {
        const pipeline = redis.pipeline();
        const clientId = await redis.incr('clienteId');
        pipeline.hmset(`cliente:${clientId}`, {
            nome: body.nome,
            limite: body.limite,
            saldo: body.saldo
        });
        await pipeline.exec();
        return res.status(201).send({ clienteId: clientId });
    } catch (err) {
        return res.status(500).send({ message: err.message });
    }
});

server.post('/clientes/:id/transacoes', async (req, res) => {
  const { body, params } = req;

  // Validação rápida do ID antes de prosseguir
  if (!parseInt(params.id)) {
    return res.status(400).send({ message: 'ID de cliente inválido' });
  }

  const bodyValidator = yup.object({
    valor: yup.number().required(),
    descricao: yup.string().max(10).required(),
    tipo: yup.string().oneOf(["d", "c"]).required(),
  });

  try {
    await bodyValidator.validate(body);
  } catch (err) {
    return res.status(400).send({ message: err.errors });
  }

  try {
    await database.tx(async t => {
      const customer = await t.one('SELECT * FROM clientes WHERE id = $1', params.id);

      const balanceUpdated = body.tipo === "d" ? customer.saldo - body.valor : customer.saldo + body.valor;
      if (balanceUpdated < -customer.limite) {
        throw new Error('Saldo insuficiente');
      }

      await t.query('INSERT INTO transacoes (cliente_id, valor, tipo, descricao, realizada_em) VALUES ($1, $2, $3, $4, NOW())', [params.id, body.valor, body.tipo, body.descricao]);
      const updateResult = await t.query('UPDATE clientes SET saldo = $1, versao = versao + 1 WHERE id = $2 AND versao = $3', [balanceUpdated, params.id, customer.versao]);
      if (updateResult.rowCount === 0) {
        throw new Error('Os dados do cliente foram alterados, tente novamente.');
      }
    });

    return res.status(200).send({
      message: 'Transação realizada com sucesso'
    });
  } catch (err) {
    switch (err.message) {
      case 'Cliente não encontrado':
        return res.status(404).send({ message: err.message });
      case 'Saldo insuficiente':
      case 'Os dados do cliente foram alterados, tente novamente.':
        return res.status(422).send({ message: err.message });
      default:
        return res.status(500).send({ message: 'Erro no servidor' });
    }
  }
});


server.get('/clientes/:id/extrato', async (req, res) => {
    const { params } = req;
    try {
        const customer = await redis.hgetall(`cliente:${params.id}`);
        if (!customer || !customer.nome) {
            return res.status(404).send();
        }

        const transactionKeys = await redis.keys(`transacao:*:cliente_id:${params.id}`);
        const transactions = await Promise.all(transactionKeys.map(key => redis.hgetall(key)));
        transactions.sort((a, b) => new Date(b.realizada_em) - new Date(a.realizada_em));

        return res.status(200).send({
            "saldo": {
                "total": customer.saldo,
                "data_extrato": new Date().toISOString(),
                "limite": customer.limite
            },
            "ultimas_transacoes": transactions.slice(0, 10)
        });
    } catch (err) {
        console.log("ERROR: /clientes/:id/extrato: ", err.message)
        return res.status(500).send({ message: err.message });
    }
});

server.listen({
    host: '0.0.0.0',
    port: process.env.PORT
}, (err, address) => {
    if (err) throw err;
    console.log(`Server is running at ${address}`);
});
