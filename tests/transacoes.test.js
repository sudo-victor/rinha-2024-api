const axios = require("axios")

describe("Transacoes", () => {
  it("should be able to add a transaction", async () => {
    const payload = {
      valor: 1000,
      descricao: "",
      tipo: "c"
    }
    const response = await axios.post("http://localhost:3333/clientes/3/transacoes", payload)
    expect(response.status).toBe(201)
  })

  it("should not be to make a debit transaction if does have balance", async () => {
    const payload = {
      valor: 1000000000,
      descricao: "",
      tipo: "d"
    }
    const response = await axios.post("http://localhost:3333/clientes/3/transacoes", payload, {
      validateStatus: () => true
    })
    expect(response.status).toBe(422)
  })

  it("should be able to list extract", async () => {
    const response = await axios.get("http://localhost:3333/clientes/3/extrato")
    console.log(response.data)
    expect(response.status).toBe(200)
  })
})