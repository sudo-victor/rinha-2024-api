const axios = require("axios")

describe("Clientes", () => {
  it.skip("should be able to add a customer", async () => {
    const payload = {
      nome: "Victor Soares",
      limite: 100000,
      saldo: 1000000
    }
    const response = await axios.post("http://localhost:3333/clientes", payload)
    expect(response.status).toBe(201)
  })
})