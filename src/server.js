const dotenv = require('dotenv');
dotenv.config();
const fastify = require("fastify");
const Redis = require('ioredis');
const redis = new Redis(process.env.REDIS_URL);
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
    if (!body.valor || !['d', 'c'].includes(body.tipo) || body.descricao.length > 10) {
        return res.status(400).send({ message: 'Invalid request data' });
    }
    try {
        const customer = await redis.hgetall(`cliente:${params.id}`);
        if (!customer || !customer.nome) throw new Error('Cliente não encontrado');

        const balanceUpdated = body.tipo === "d" ? Number(customer.saldo) - body.valor : Number(customer.saldo) + body.valor;
        if (balanceUpdated < -customer.limite) throw new Error('Saldo insuficiente');

        const pipeline = redis.pipeline();
        const transactionId = await redis.incr('transactionId');
        pipeline.hmset(`transacao:${transactionId}`, {
            cliente_id: params.id,
            valor: body.valor,
            tipo: body.tipo,
            descricao: body.descricao,
            realizada_em: new Date().toISOString()
        });
        pipeline.hset(`cliente:${params.id}`, 'saldo', balanceUpdated);
        await pipeline.exec();

        return res.status(200).send({ saldo: balanceUpdated, limite: customer.limite });
    } catch (err) {
        console.log("ERROR: /clientes/:id/transacoes: ", err.message)
        return res.status(500).send({ message: err.message });
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
