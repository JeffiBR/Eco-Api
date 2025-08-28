const express = require("express");
const { main } = require("./coletor");

const app = express();
const PORT = process.env.PORT || 3000;

app.get("/", (req, res) => {
  res.send("✅ Coletor rodando no Render!");
});

app.get("/rodar", async (req, res) => {
  try {
    await main();
    res.send("Coleta executada e salva no banco!");
  } catch (err) {
    res.status(500).send("Erro: " + err.message);
  }
});

app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});

