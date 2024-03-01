# Features

1. POST /clientes/[id]/transacoes - ok

{
    "valor": 1000,
    "tipo" : "c",
    "descricao" : "descricao"
}

2. GET /clientes/[id]/extrato

{
  "saldo": {
    "total": -9098,
    "data_extrato": "2024-01-17T02:34:41.217753Z",
    "limite": 100000
  },
  "ultimas_transacoes": [
    {
      "valor": 10,
      "tipo": "c",
      "descricao": "descricao",
      "realizada_em": "2024-01-17T02:34:38.543030Z"
    },
    {
      "valor": 90000,
      "tipo": "d",
      "descricao": "descricao",
      "realizada_em": "2024-01-17T02:34:38.543030Z"
    }
  ]
}

3. POST /clientes - ok

{
  id: "asd",
  limite: 1000,
  saldo: 1000
}