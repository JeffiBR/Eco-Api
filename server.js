const express = require("express");
const cron = require("node-cron");
const { main } = require("./coletor");

const app = express();
const PORT = process.env.PORT || 3000;

// Rota padrão
app.get("/", (req, res) => {
  res.send("✅ Coletor rodando no Render!");
});

// Rota para rodar manualmente
app.get("/rodar", async (req, res) => {
  try {
    await main();
    res.send("Coleta executada e salva no banco!");
  } catch (err) {
    res.status(500).send("Erro: " + err.message);
  }
});

// ===== Cron diário =====
// Executa todo dia às 13:10
cron.schedule('17 13 * * *', async () => {
  console.log('🕐 Iniciando coleta diária às 13:10...');
  try {
    await main();
    console.log('✅ Coleta diária finalizada com sucesso!');
  } catch (err) {
    console.error('❌ Erro na coleta diária:', err.message);
  }
}, {
  timezone: "America/Maceio" // horário de Alagoas
});

// Inicia servidor
app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
